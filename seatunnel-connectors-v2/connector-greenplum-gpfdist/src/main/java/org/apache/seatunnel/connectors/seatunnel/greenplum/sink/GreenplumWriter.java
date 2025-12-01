/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.greenplum.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.greenplum.client.GreenplumConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.greenplum.exception.GreenplumConnectorException;
import org.apache.seatunnel.connectors.seatunnel.greenplum.gpfdist.BufferExchange;
import org.apache.seatunnel.connectors.seatunnel.greenplum.gpfdist.GpfdistServer;
import org.apache.seatunnel.connectors.seatunnel.greenplum.serde.RowDataToTextConverter;
import org.apache.seatunnel.connectors.seatunnel.greenplum.sink.state.GreenplumCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.greenplum.utils.NetworkUtils;
import org.apache.seatunnel.connectors.seatunnel.greenplum.utils.ProgressTracker;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED;

@Slf4j
public class GreenplumWriter implements SinkWriter<SeaTunnelRow, GreenplumCommitInfo, Void> {

    private final GreenplumSinkOptions options;
    private final SeaTunnelRowType rowType;
    private final GreenplumConnectionProvider connectionProvider;
    private final RowDataToTextConverter converter;
    private final BufferExchange bufferExchange;
    private final ProgressTracker progressTracker = new ProgressTracker();

    private final AtomicLong recordCount = new AtomicLong(0);
    private final AtomicBoolean sqlThreadStarted = new AtomicBoolean(false);
    private final AtomicBoolean writerClosed = new AtomicBoolean(false);
    private final List<String> batchBuffer = new ArrayList<>();

    private GpfdistServer gpfdistServer;
    private CompletableFuture<Void> sqlExecutionFuture;
    private String externalTableName;
    private volatile long lastFlushTime;

    private final String instanceId;
    private final NetworkUtils.HostInfo hostInfo;

    public GreenplumWriter(GreenplumSinkOptions options, SeaTunnelRowType rowType) {
        this.options = options;
        this.rowType = rowType;
        this.connectionProvider =
                GreenplumConnectionProvider.builder()
                        .jdbcUrl(options.getUrl())
                        .username(options.getUsername())
                        .password(options.getPassword())
                        .build();

        this.converter =
                new RowDataToTextConverter(
                        rowType, options.getDelimiter(), options.getNullString());
        this.bufferExchange = new BufferExchange(options.getBufferSize());
        this.lastFlushTime = System.currentTimeMillis();

        this.instanceId = generateInstanceId();
        this.hostInfo = NetworkUtils.getInstance().getLocalHostNameAndIp();

        log.info(
                "GreenplumWriter created: instanceId={}, host={}",
                instanceId,
                hostInfo.getIpAddress());

        if (options.isEnableGpfdist()) {
            initializeGpfdist();
        }

        validateOptions();
    }

    @Override
    public void write(SeaTunnelRow element) {
        if (writerClosed.get()) {
            throw new GreenplumConnectorException(
                    WRITER_OPERATION_FAILED, "Writer is already closed");
        }

        try {
            String textData =
                    progressTracker.trackProgress("convert_row", () -> converter.convert(element));

            if (options.isEnableGpfdist()) {

                byte[] dataBytes = (textData + '\n').getBytes(StandardCharsets.UTF_8);

                progressTracker.trackProgress("buffer_write", () -> bufferExchange.put(dataBytes));
                progressTracker.trackBytes("buffer_write", dataBytes.length);

                if (sqlThreadStarted.compareAndSet(false, true)) {
                    startSqlExecution();
                }
            } else {

                synchronized (batchBuffer) {
                    batchBuffer.add(textData);
                }
            }

            recordCount.incrementAndGet();
            progressTracker.incrementCount("records_written");

            if (shouldFlush()) {
                flush();
            }

        } catch (Exception e) {
            throw new GreenplumConnectorException(
                    WRITER_OPERATION_FAILED, "Failed to write record", e);
        }
    }

    @Override
    public Optional<GreenplumCommitInfo> prepareCommit() {
        try {
            log.debug("Preparing commit for instance: {}", instanceId);

            if (options.isEnableGpfdist()) {

                progressTracker.trackProgress(
                        "prepare_commit_gpfdist",
                        () -> {
                            try {

                                bufferExchange.flush();
                                if (gpfdistServer != null) {
                                    gpfdistServer.endTransfer();
                                }

                                if (sqlExecutionFuture != null) {
                                    sqlExecutionFuture.get(
                                            options.getQueryTimeout(), TimeUnit.SECONDS);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to prepare commit in GPfdist mode", e);
                            }
                        });
            } else {

                if (!batchBuffer.isEmpty()) {
                    progressTracker.trackProgress("flush_final_batch", this::flush);
                }
            }

            long count = recordCount.getAndSet(0);
            log.info(
                    "Prepare commit completed for instance {}: {} records processed, {}",
                    instanceId,
                    count,
                    progressTracker.reportSimple());

            return Optional.of(new GreenplumCommitInfo(count));

        } catch (Exception e) {
            throw new GreenplumConnectorException(
                    WRITER_OPERATION_FAILED, "Failed to prepare commit", e);
        }
    }

    @Override
    public void abortPrepare() {
        log.warn("Aborting prepare for instance: {}", instanceId);

        try {

            synchronized (batchBuffer) {
                batchBuffer.clear();
            }
            bufferExchange.close();
            recordCount.set(0);

            if (sqlExecutionFuture != null && !sqlExecutionFuture.isDone()) {
                sqlExecutionFuture.cancel(true);
            }

            cleanupExternalTable();

        } catch (Exception e) {
            log.warn("Error during abort prepare: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        if (writerClosed.compareAndSet(false, true)) {
            try {
                log.info("Closing GreenplumWriter: {}", instanceId);

                if (!batchBuffer.isEmpty()) {
                    progressTracker.trackProgress("final_flush", this::flush);
                }

                if (gpfdistServer != null) {
                    progressTracker.trackProgress(
                            "stop_gpfdist",
                            () -> {
                                gpfdistServer.endTransfer();

                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {

                                }
                                gpfdistServer.stop();
                            });
                }

                bufferExchange.close();

                cleanupExternalTable();

                log.info(
                        "GreenplumWriter closed: {}, final stats: {}",
                        instanceId,
                        progressTracker.reportTimeTaken());

            } catch (Exception e) {
                log.warn("Error closing GreenplumWriter: {}", e.getMessage());
            }
        }
    }

    private void initializeGpfdist() {
        try {
            String bindAddress = options.getBindAddress();
            String externalHost = options.getExternalHost();

            if (externalHost == null) {
                externalHost = hostInfo.getIpAddress();
            }

            gpfdistServer =
                    new GpfdistServer(
                            options.getGpfdistPort(), bindAddress, externalHost, bufferExchange);
            gpfdistServer.start();

            externalTableName = generateExternalTableName();

            log.info(
                    "GPfdist server initialized: {} -> {}",
                    gpfdistServer.getUrl(),
                    externalTableName);

        } catch (Exception e) {
            throw new GreenplumConnectorException(
                    WRITER_OPERATION_FAILED, "Failed to initialize GPfdist server", e);
        }
    }

    private void startSqlExecution() {
        sqlExecutionFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                log.debug("Starting SQL execution for instance: {}", instanceId);

                                try (Connection conn = connectionProvider.getConnection()) {
                                    conn.setAutoCommit(false);

                                    progressTracker.trackProgress(
                                            "wait_gpfdist_ready", () -> waitForGpfdistReady());

                                    progressTracker.trackProgress(
                                            "create_external_table",
                                            () -> createExternalTable(conn));

                                    progressTracker.trackProgress(
                                            "execute_insert",
                                            () -> executeInsertFromExternal(conn));

                                    progressTracker.trackProgress(
                                            "commit_transaction", () -> conn.commit());

                                    log.info(
                                            "SQL execution completed successfully for instance: {}",
                                            instanceId);

                                } catch (Exception e) {
                                    log.error(
                                            "SQL execution failed for instance: {}", instanceId, e);
                                    throw e;
                                }
                            } catch (Exception e) {
                                throw new RuntimeException("SQL execution failed", e);
                            }
                        });
    }

    private void waitForGpfdistReady() throws InterruptedException {
        if (gpfdistServer == null) {
            throw new IllegalStateException("GPfdist server not initialized");
        }

        int maxWaitSeconds = 30;
        for (int i = 0; i < maxWaitSeconds; i++) {
            if (gpfdistServer.isHealthy()
                    && NetworkUtils.getInstance()
                            .isGpfdistHealthy(hostInfo.getIpAddress(), gpfdistServer.getPort())) {
                log.debug("GPfdist server is ready: {}", gpfdistServer.getUrl());
                return;
            }
            Thread.sleep(1000);
        }

        throw new RuntimeException("GPfdist server not ready after " + maxWaitSeconds + " seconds");
    }

    private void createExternalTable(Connection conn) throws SQLException {
        String createSql = buildCreateExternalTableSQL();

        try (Statement stmt = conn.createStatement()) {
            log.debug("Creating external table: {}", createSql);
            stmt.execute(createSql);
            log.debug("External table created successfully: {}", externalTableName);
        }
    }

    private String buildCreateExternalTableSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE WRITABLE EXTERNAL ");

        sql.append("TABLE ").append(externalTableName).append(" (");

        String[] fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) sql.append(", ");
            sql.append(fieldNames[i]).append(" TEXT");
        }

        sql.append(") LOCATION ('").append(gpfdistServer.getUrl()).append("') ");
        sql.append("FORMAT 'TEXT' (");
        sql.append("DELIMITER '").append(escapeString(options.getDelimiter())).append("' ");
        sql.append("NULL '").append(escapeString(options.getNullString())).append("' ");
        sql.append("ESCAPE 'OFF' ");
        sql.append(") ");
        sql.append("ENCODING '").append(options.getEncoding()).append("'");

        return sql.toString();
    }

    private void executeInsertFromExternal(Connection conn) throws SQLException {
        String insertSql = buildInsertFromExternalSQL();

        try (Statement stmt = conn.createStatement()) {

            stmt.setQueryTimeout(options.getQueryTimeout());

            log.debug("Executing insert from external table: {}", insertSql);

            long startTime = System.currentTimeMillis();
            int rowCount = stmt.executeUpdate(insertSql);
            long duration = System.currentTimeMillis() - startTime;

            log.info(
                    "Insert completed: {} rows in {}ms for instance: {}",
                    rowCount,
                    duration,
                    instanceId);

            progressTracker.incrementCount("rows_inserted", rowCount);
        }
    }

    private String buildInsertFromExternalSQL() {
        StringBuilder sql = new StringBuilder();

        if (options.getPreWriteSql() != null) {
            sql.append(options.getPreWriteSql()).append("; ");
        }

        sql.append("INSERT INTO ");
        sql.append(options.getDatabase()).append(".").append(options.getTableName());
        sql.append(" SELECT * FROM ").append(externalTableName);

        if (options.getPostWriteSql() != null) {
            sql.append("; ").append(options.getPostWriteSql());
        }

        return sql.toString();
    }

    private void cleanupExternalTable() {
        if (externalTableName != null) {
            try (Connection conn = connectionProvider.getConnection()) {
                progressTracker.trackProgress(
                        "cleanup_external_table",
                        () -> {
                            try {
                                dropExternalTable(conn);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
            } catch (Exception e) {
                log.warn("Failed to cleanup external table: {}", externalTableName, e);
            }
        }
    }

    private void dropExternalTable(Connection conn) throws SQLException {
        String dropSql = "DROP EXTERNAL TABLE IF EXISTS " + externalTableName;

        try (Statement stmt = conn.createStatement()) {
            log.debug("Dropping external table: {}", dropSql);
            stmt.execute(dropSql);
            log.debug("External table dropped: {}", externalTableName);
        }
    }

    private boolean shouldFlush() {
        if (options.isEnableGpfdist()) {
            return false;
        }

        long now = System.currentTimeMillis();
        synchronized (batchBuffer) {
            return batchBuffer.size() >= options.getFlushMaxRows()
                    || (now - lastFlushTime) >= options.getFlushIntervalMs();
        }
    }

    private void flush() {
        if (options.isEnableGpfdist()) {
            bufferExchange.flush();
            return;
        }

        List<String> currentBatch;
        synchronized (batchBuffer) {
            if (batchBuffer.isEmpty()) {
                return;
            }
            currentBatch = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
        }

        try (Connection conn = connectionProvider.getConnection()) {
            progressTracker.trackProgress(
                    "jdbc_flush",
                    () -> {
                        try {
                            flushWithJdbc(conn, currentBatch);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
            lastFlushTime = System.currentTimeMillis();

        } catch (Exception e) {
            throw new GreenplumConnectorException(
                    WRITER_OPERATION_FAILED, "Failed to flush data via JDBC", e);
        }
    }

    private void flushWithJdbc(Connection conn, List<String> batch) throws SQLException {
        if (batch.isEmpty()) {
            return;
        }

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ")
                .append(options.getDatabase())
                .append(".")
                .append(options.getTableName())
                .append(" VALUES ");

        for (int i = 0; i < batch.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("(").append(batch.get(i)).append(")");
        }

        try (Statement stmt = conn.createStatement()) {
            int affected = stmt.executeUpdate(sql.toString());
            log.debug("JDBC flush completed: {} records for instance: {}", affected, instanceId);
            progressTracker.incrementCount("jdbc_batch_flush");
            progressTracker.incrementCount("jdbc_rows_written", affected);
        }
    }

    private String generateInstanceId() {
        return String.format(
                "writer_%s_%d_%d",
                UUID.randomUUID().toString().replaceAll("-", "").substring(0, 8),
                Thread.currentThread().getId(),
                System.currentTimeMillis());
    }

    private String generateExternalTableName() {
        return String.format("st_ext_w_%s", UUID.randomUUID().toString().replaceAll("-", ""));
    }

    private String escapeString(String str) {
        if (str == null) return "";
        return str.replace("'", "''");
    }

    private void validateOptions() {
        if (options.getUrl() == null || options.getUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("JDBC URL cannot be null or empty");
        }
        if (options.getTableName() == null || options.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (options.isEnableGpfdist() && options.getGpfdistPort() < 0) {
            throw new IllegalArgumentException(
                    "GPfdist port must be non-negative when GPfdist is enabled");
        }

        try {
            connectionProvider.testConnection();
            log.debug("Database connection test passed for instance: {}", instanceId);
        } catch (Exception e) {
            throw new GreenplumConnectorException(
                    WRITER_OPERATION_FAILED, "Failed to connect to database", e);
        }
    }
}
