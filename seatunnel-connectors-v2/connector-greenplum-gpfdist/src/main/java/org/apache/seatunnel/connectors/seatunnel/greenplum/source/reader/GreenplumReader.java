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

package org.apache.seatunnel.connectors.seatunnel.greenplum.source.reader;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.greenplum.client.GreenplumConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.greenplum.config.GreenplumConfig;
import org.apache.seatunnel.connectors.seatunnel.greenplum.exception.GreenplumConnectorException;
import org.apache.seatunnel.connectors.seatunnel.greenplum.gpfdist.BufferExchange;
import org.apache.seatunnel.connectors.seatunnel.greenplum.gpfdist.GpfdistServer;
import org.apache.seatunnel.connectors.seatunnel.greenplum.serde.TextToRowDataConverter;
import org.apache.seatunnel.connectors.seatunnel.greenplum.source.split.GreenplumSplit;
import org.apache.seatunnel.connectors.seatunnel.greenplum.utils.NetworkUtils;
import org.apache.seatunnel.connectors.seatunnel.greenplum.utils.ProgressTracker;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.READER_OPERATION_FAILED;

@Slf4j
public class GreenplumReader implements SourceReader<SeaTunnelRow, GreenplumSplit> {

    private final SourceReader.Context context;
    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;
    private final GreenplumConnectionProvider connectionProvider;
    private final TextToRowDataConverter converter;
    private final ProgressTracker progressTracker = new ProgressTracker();

    private final Queue<GreenplumSplit> pendingSplits = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean noMoreSplit = new AtomicBoolean(false);
    private final AtomicBoolean readerClosed = new AtomicBoolean(false);
    private final AtomicLong processedRecords = new AtomicLong(0);

    private GpfdistServer gpfdistServer;
    private BufferExchange bufferExchange;
    private final AtomicBoolean gpfdistInitialized = new AtomicBoolean(false);

    private final String instanceId;
    private final NetworkUtils.HostInfo hostInfo;
    private final String delimiter;
    private final String nullString;
    private final boolean enableGpfdist;

    public GreenplumReader(
            SourceReader.Context context, ReadonlyConfig config, CatalogTable catalogTable) {
        this.context = context;
        this.config = config;
        this.catalogTable = catalogTable;
        this.connectionProvider =
                GreenplumConnectionProvider.builder()
                        .jdbcUrl(config.get(GreenplumConfig.JDBC_URL))
                        .username(config.get(GreenplumConfig.USERNAME))
                        .password(config.get(GreenplumConfig.PASSWORD))
                        .build();

        this.converter =
                new TextToRowDataConverter(
                        catalogTable.getSeaTunnelRowType(),
                        config.getOptional(GreenplumConfig.DELIMITER).orElse("\t"));

        this.delimiter = config.getOptional(GreenplumConfig.DELIMITER).orElse("\t");
        this.nullString = config.getOptional(GreenplumConfig.NULL_STRING).orElse("\\N");
        this.enableGpfdist = config.getOptional(GreenplumConfig.ENABLE_GPFDIST).orElse(true);

        this.instanceId = generateInstanceId();
        this.hostInfo = NetworkUtils.getInstance().getLocalHostNameAndIp();

        log.info(
                "GreenplumReader created: instanceId={}, host={}, gpfdist={}",
                instanceId,
                hostInfo.getIpAddress(),
                enableGpfdist);
    }

    @Override
    public void open() throws Exception {
        log.info("Opening GreenplumReader: {}", instanceId);

        try {
            connectionProvider.testConnection();
            log.debug("Database connection test passed for reader: {}", instanceId);
        } catch (Exception e) {
            throw new GreenplumConnectorException(
                    READER_OPERATION_FAILED, "Failed to connect to database", e);
        }

        if (enableGpfdist) {
            initializeGpfdist();
        }
    }

    @Override
    public void close() throws IOException {
        if (readerClosed.compareAndSet(false, true)) {
            log.info(
                    "Closing GreenplumReader: {}, processed {} records",
                    instanceId,
                    processedRecords.get());

            try {

                if (gpfdistServer != null) {
                    progressTracker.trackProgress("stop_gpfdist", gpfdistServer::stop);
                }

                if (bufferExchange != null) {
                    bufferExchange.close();
                }

                log.info(
                        "GreenplumReader closed: {}, final stats: {}",
                        instanceId,
                        progressTracker.reportTimeTaken());

            } catch (Exception e) {
                log.warn("Error closing GreenplumReader: {}", e.getMessage());
            }
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (readerClosed.get()) {
            return;
        }

        GreenplumSplit currentSplit = pendingSplits.poll();
        if (currentSplit != null) {
            log.info(
                    "Processing split: {} for instance: {}", currentSplit.getSplitId(), instanceId);

            progressTracker.trackProgress(
                    "process_split",
                    () -> {
                        try {
                            if (enableGpfdist) {
                                readDataWithGpfdist(currentSplit, output);
                            } else {
                                readDataWithJdbc(currentSplit, output);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to process split: " + currentSplit.getSplitId(), e);
                        }
                    });
        }

        if (noMoreSplit.get() && pendingSplits.isEmpty()) {
            log.info(
                    "No more splits, signaling completion. Total processed: {} for instance: {}",
                    processedRecords.get(),
                    instanceId);
            context.signalNoMoreElement();
        }
    }

    private void readDataWithGpfdist(GreenplumSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        String extTableName = generateExternalTableName(split.getSplitId());

        try {

            ensureGpfdistInitialized();

            progressTracker.trackProgress(
                    "create_readable_table",
                    () -> {
                        try {
                            createReadableExternalTable(extTableName);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

            CompletableFuture<Void> queryFuture =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    executeQueryToGpfdist(split, extTableName);
                                } catch (Exception e) {
                                    log.error(
                                            "Query execution failed for split: {}",
                                            split.getSplitId(),
                                            e);
                                    throw new RuntimeException(e);
                                }
                            });

            progressTracker.trackProgress(
                    "read_process_data",
                    () -> {
                        try {
                            readAndProcessData(output);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

            queryFuture.get(
                    config.getOptional(GreenplumConfig.QUERY_TIMEOUT).orElse(300),
                    TimeUnit.SECONDS);

        } finally {

            progressTracker.trackProgress(
                    "cleanup_table",
                    () -> {
                        try {
                            dropExternalTable(extTableName);
                        } catch (Exception e) {
                            log.warn("Failed to drop external table: {}", extTableName, e);
                        }
                    });
        }
    }

    private void readDataWithJdbc(GreenplumSplit split, Collector<SeaTunnelRow> output)
            throws Exception {

        String query = buildJdbcQuery(split);

        try (Connection conn = connectionProvider.getConnection();
                Statement stmt = conn.createStatement()) {

            stmt.setFetchSize(config.getOptional(GreenplumConfig.FETCH_SIZE).orElse(1000));
            stmt.setQueryTimeout(config.getOptional(GreenplumConfig.QUERY_TIMEOUT).orElse(300));

            log.debug("Executing JDBC query for split {}: {}", split.getSplitId(), query);

            long startTime = System.currentTimeMillis();

            try (ResultSet rs = stmt.executeQuery(query)) {
                SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
                long recordCount = 0;

                while (rs.next() && !readerClosed.get()) {
                    SeaTunnelRow row = convertResultSetToRow(rs, rowType);
                    output.collect(row);
                    recordCount++;

                    if (recordCount % 10000 == 0) {
                        log.debug(
                                "Processed {} records for split: {}",
                                recordCount,
                                split.getSplitId());
                    }
                }

                long duration = System.currentTimeMillis() - startTime;
                processedRecords.addAndGet(recordCount);

                log.info(
                        "JDBC read completed for split {}: {} records in {}ms",
                        split.getSplitId(),
                        recordCount,
                        duration);
            }
        }
    }

    private void initializeGpfdist() throws Exception {
        if (gpfdistInitialized.compareAndSet(false, true)) {
            try {
                int port = config.getOptional(GreenplumConfig.GPFDIST_PORT).orElse(0);
                String bindAddress =
                        config.getOptional(GreenplumConfig.GPFDIST_BIND_ADDRESS).orElse("0.0.0.0");
                String externalHost =
                        config.getOptional(GreenplumConfig.GPFDIST_HOST)
                                .orElse(hostInfo.getIpAddress());

                int bufferSize = config.getOptional(GreenplumConfig.BUFFER_SIZE).orElse(8192);
                bufferExchange = new BufferExchange(bufferSize);

                gpfdistServer = new GpfdistServer(port, bindAddress, externalHost, bufferExchange);
                gpfdistServer.start();

                log.info(
                        "GPfdist server initialized for reader: {} on port: {}",
                        instanceId,
                        gpfdistServer.getPort());

            } catch (Exception e) {
                gpfdistInitialized.set(false);
                throw new GreenplumConnectorException(
                        READER_OPERATION_FAILED, "Failed to initialize GPfdist server", e);
            }
        }
    }

    private void ensureGpfdistInitialized() throws Exception {
        if (!gpfdistInitialized.get()) {
            initializeGpfdist();
        }

        if (!gpfdistServer.isHealthy()) {
            throw new IllegalStateException("GPfdist server is not healthy");
        }
    }

    private void createReadableExternalTable(String tableName) throws SQLException {
        String createSql = buildCreateReadableExternalTableSQL(tableName);

        try (Connection conn = connectionProvider.getConnection();
                Statement stmt = conn.createStatement()) {

            log.debug("Creating readable external table: {}", createSql);
            stmt.execute(createSql);
            log.debug("Readable external table created: {}", tableName);
        }
    }

    private String buildCreateReadableExternalTableSQL(String tableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE READABLE EXTERNAL TEMPORARY TABLE ").append(tableName).append(" (");

        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        String[] fieldNames = rowType.getFieldNames();

        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) sql.append(", ");
            sql.append(fieldNames[i]).append(" TEXT");
        }

        sql.append(") LOCATION ('").append(gpfdistServer.getUrl()).append("') ");
        sql.append("FORMAT 'TEXT' (");
        sql.append("DELIMITER '").append(escapeString(delimiter)).append("' ");
        sql.append("NULL '").append(escapeString(nullString)).append("' ");
        sql.append("ESCAPE 'OFF' ");
        sql.append(") ");
        sql.append("ENCODING 'UTF-8'");

        return sql.toString();
    }

    private void executeQueryToGpfdist(GreenplumSplit split, String extTableName)
            throws SQLException {
        String insertSql = buildInsertToExternalTableSQL(split, extTableName);

        try (Connection conn = connectionProvider.getConnection();
                Statement stmt = conn.createStatement()) {

            stmt.setQueryTimeout(config.getOptional(GreenplumConfig.QUERY_TIMEOUT).orElse(300));

            log.debug("Executing query to GPfdist for split {}: {}", split.getSplitId(), insertSql);

            long startTime = System.currentTimeMillis();
            int rowCount = stmt.executeUpdate(insertSql);
            long duration = System.currentTimeMillis() - startTime;

            log.info(
                    "Query completed for split {}: {} rows in {}ms",
                    split.getSplitId(),
                    rowCount,
                    duration);

            progressTracker.incrementCount("query_rows", rowCount);

        } finally {

            if (gpfdistServer != null) {
                gpfdistServer.endTransfer();
            }
        }
    }

    private String buildInsertToExternalTableSQL(GreenplumSplit split, String extTableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(extTableName).append(" ");

        String sourceQuery = split.getQuery();
        if (sourceQuery != null && !sourceQuery.trim().isEmpty()) {

            sql.append(sourceQuery);
        } else {

            sql.append("SELECT ");
            String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
            for (int i = 0; i < fieldNames.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append(fieldNames[i]);
            }

            sql.append(" FROM ").append(split.getTableName());

            if (split.getWhereClause() != null && !split.getWhereClause().trim().isEmpty()) {
                sql.append(" WHERE ").append(split.getWhereClause());
            }
        }

        return sql.toString();
    }

    private void readAndProcessData(Collector<SeaTunnelRow> output) throws Exception {
        long recordCount = 0;
        long startTime = System.currentTimeMillis();

        try {
            while (!readerClosed.get()) {
                ByteBuffer buffer = bufferExchange.get(1000);

                if (buffer == null) {
                    if (bufferExchange.isTransferComplete()) {
                        log.debug("Transfer completed for instance: {}", instanceId);
                        break;
                    }
                    continue;
                }

                try {
                    recordCount += processBuffer(buffer, output);

                    if (recordCount % 10000 == 0) {
                        log.debug("Processed {} records for instance: {}", recordCount, instanceId);
                    }
                } finally {
                    bufferExchange.recycleBuffer(buffer);
                }
            }
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            processedRecords.addAndGet(recordCount);

            log.info(
                    "Read and process completed: {} records in {}ms for instance: {}",
                    recordCount,
                    duration,
                    instanceId);
        }
    }

    private long processBuffer(ByteBuffer buffer, Collector<SeaTunnelRow> output) throws Exception {
        long count = 0;

        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        String content = new String(data, StandardCharsets.UTF_8);
        String[] lines = content.split("\n");

        for (String line : lines) {
            if (!line.trim().isEmpty() && !readerClosed.get()) {
                try {
                    SeaTunnelRow row = converter.convert(line);
                    output.collect(row);
                    count++;
                } catch (Exception e) {
                    log.warn("Failed to parse line: {} for instance: {}", line, instanceId, e);
                }
            }
        }

        return count;
    }

    private void dropExternalTable(String tableName) throws SQLException {
        String dropSql = "DROP EXTERNAL TABLE IF EXISTS " + tableName;

        try (Connection conn = connectionProvider.getConnection();
                Statement stmt = conn.createStatement()) {

            log.debug("Dropping external table: {}", dropSql);
            stmt.execute(dropSql);
            log.debug("External table dropped: {}", tableName);
        }
    }

    private String buildJdbcQuery(GreenplumSplit split) {
        if (split.getQuery() != null && !split.getQuery().trim().isEmpty()) {
            return split.getQuery();
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) sql.append(", ");
            sql.append(fieldNames[i]);
        }

        sql.append(" FROM ").append(split.getTableName());

        if (split.getWhereClause() != null && !split.getWhereClause().trim().isEmpty()) {
            sql.append(" WHERE ").append(split.getWhereClause());
        }

        return sql.toString();
    }

    private SeaTunnelRow convertResultSetToRow(ResultSet rs, SeaTunnelRowType rowType)
            throws SQLException {
        SeaTunnelRow row = new SeaTunnelRow(rowType.getTotalFields());

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            Object value = rs.getObject(i + 1);
            row.setField(i, value);
        }

        return row;
    }

    private String generateInstanceId() {
        return String.format(
                "reader_%d_%s",
                context.getIndexOfSubtask(),
                UUID.randomUUID().toString().replaceAll("-", "").substring(0, 8));
    }

    private String generateExternalTableName(String splitId) {
        return String.format("st_ext_r_%s_%s", splitId, instanceId).toLowerCase();
    }

    private String escapeString(String str) {
        if (str == null) return "";
        return str.replace("'", "''");
    }

    @Override
    public List<GreenplumSplit> snapshotState(long checkpointId) throws Exception {
        log.debug(
                "Snapshotting state for checkpoint: {}, pending splits: {} for instance: {}",
                checkpointId,
                pendingSplits.size(),
                instanceId);
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<GreenplumSplit> splits) {
        if (splits != null && !splits.isEmpty()) {
            log.info("Adding {} splits to reader instance: {}", splits.size(), instanceId);
            pendingSplits.addAll(splits);
        }
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("No more splits signal received for instance: {}", instanceId);
        noMoreSplit.set(true);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        log.debug("Checkpoint {} completed for instance: {}", checkpointId, instanceId);
    }
}
