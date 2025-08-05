package org.apache.seatunnel.connectors.greenplum.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.greenplum.config.GreenplumConfig;
import org.apache.seatunnel.connectors.greenplum.gpfdist.DataTransferService;
import org.apache.seatunnel.connectors.greenplum.gpfdist.GpfdistServer;
import org.apache.seatunnel.connectors.greenplum.util.GreenplumUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class GreenplumSinkWriter implements SinkWriter<SeaTunnelRow, GreenplumCommitInfo, GreenplumSinkState> {
    private static final Logger LOG = LoggerFactory.getLogger(GreenplumSinkWriter.class);
    
    private final GreenplumConfig config;
    private final CatalogTable catalogTable;
    private final Context context;
    private final String writerId;
    
    private final BlockingQueue<ByteBuffer> dataQueue;
    private final List<String> buffer;
    private final GpfdistServer gpfdistServer;
    private final DataTransferService dataService;
    
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean hasData = new AtomicBoolean(false);
    private final AtomicLong totalRows = new AtomicLong(0);
    private final CountDownLatch writerLatch = new CountDownLatch(1);
    
    private Thread writerThread;
    private String externalTableName;
    private volatile Exception lastError;
    
    public GreenplumSinkWriter(
            GreenplumConfig config, 
            CatalogTable catalogTable,
            Context context) throws IOException {
        this.config = config;
        this.catalogTable = catalogTable;
        this.context = context;
        this.writerId = UUID.randomUUID().toString();
        
        this.dataQueue = new LinkedBlockingQueue<>(config.getQueueSize());
        this.buffer = new ArrayList<>(config.getBatchSize());
        
        // Initialize data transfer service
        this.dataService = new WriterDataTransferService();
        
        // Start GPFDIST server
        this.gpfdistServer = new GpfdistServer(0, dataService);
        this.gpfdistServer.start();
        
        LOG.info("GPFDIST server started on port: {} for writer: {}", 
                gpfdistServer.getActualPort(), writerId);
        
        // Create external table
        createExternalTable();
        
        // Start writer thread
        startWriterThread();
    }
    
    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkError();
        
        String rowData = GreenplumUtil.rowToString(element, config.getDelimiter(), config.getNullString()) + "\n";
        buffer.add(rowData);
        
        if (buffer.size() >= config.getBatchSize()) {
            flushBuffer();
        }
    }
    
    @Override
    public Optional<GreenplumCommitInfo> prepareCommit() throws IOException {
        LOG.info("Preparing commit for writer: {}", writerId);
        
        // Flush any remaining data
        flushBuffer();
        
        // Mark that we have data to commit
        if (totalRows.get() > 0) {
            hasData.set(true);
            return Optional.of(new GreenplumCommitInfo(
                writerId,
                externalTableName,
                totalRows.get(),
                System.currentTimeMillis()
            ));
        }
        
        return Optional.empty();
    }
    
    @Override
    public List<GreenplumSinkState> snapshotState(long checkpointId) throws IOException {
        return Collections.singletonList(new GreenplumSinkState(
            writerId,
            totalRows.get(),
            checkpointId
        ));
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Closing sink writer: {}", writerId);
        
        running.set(false);
        
        try {
            // Wait for writer thread to finish
            if (!writerLatch.await(30, TimeUnit.SECONDS)) {
                LOG.warn("Writer thread did not finish in time");
            }
            
            if (writerThread != null) {
                writerThread.interrupt();
                writerThread.join(5000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Stop GPFDIST server
        if (gpfdistServer != null) {
            gpfdistServer.stop();
        }
        
        // Drop external table
        dropExternalTable();
        
        checkError();
    }
    
    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        
        try {
            StringBuilder sb = new StringBuilder();
            for (String row : buffer) {
                sb.append(row);
            }
            
            byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
            ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            
            if (!dataQueue.offer(byteBuffer, 60, TimeUnit.SECONDS)) {
                throw new IOException("Failed to write data to queue: timeout");
            }
            
            totalRows.addAndGet(buffer.size());
            buffer.clear();
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing data", e);
        }
    }
    
    private void startWriterThread() {
        writerThread = new Thread(() -> {
            LOG.info("Writer thread started for writer: {}", writerId);
            
            try {
                while (running.get() || hasData.get()) {
                    if (hasData.compareAndSet(true, false)) {
                        executeInsert();
                    } else {
                        Thread.sleep(100);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error in writer thread", e);
                lastError = e;
            } finally {                 writerLatch.countDown();
                LOG.info("Writer thread finished for writer: {}", writerId);
            }
        });
        
        writerThread.setName("GP-Writer-" + writerId);
        writerThread.setDaemon(true);
        writerThread.start();
    }
    
    private void createExternalTable() throws IOException {
        externalTableName = "gpfdist_sink_" + System.currentTimeMillis() + "_" + 
                           context.getIndexOfSubtask();
        
        String columns = config.getColumnDefinitions() != null ? 
            config.getColumnDefinitions() : 
            GreenplumUtil.generateColumnDefinitions(catalogTable);
        
        String createTableSql = String.format(
            "CREATE EXTERNAL TABLE %s (%s) " +
            "LOCATION ('gpfdist://%s:%d/*') " +
            "FORMAT 'TEXT' (DELIMITER '%s' NULL '%s')",
            externalTableName,
            columns,
            GreenplumUtil.getLocalHost(),
            gpfdistServer.getActualPort(),
            config.getDelimiter(),
            config.getNullString()
        );
        
        LOG.info("Creating external table: {}", createTableSql);
        executeSQL(createTableSql);
    }
    
    private void dropExternalTable() {
        if (externalTableName == null) {
            return;
        }
        
        try {
            String dropTableSql = "DROP EXTERNAL TABLE IF EXISTS " + externalTableName;
            executeSQL(dropTableSql);
            LOG.info("Dropped external table: {}", externalTableName);
        } catch (Exception e) {
            LOG.warn("Failed to drop external table", e);
        }
    }
    
    private void executeInsert() throws Exception {
        String insertSql = String.format(
            "INSERT INTO %s SELECT * FROM %s",
            config.getTableName(),
            externalTableName
        );
        
        LOG.info("Executing insert: {}", insertSql);
        
        long startTime = System.currentTimeMillis();
        try (Connection conn = GreenplumUtil.getConnection(config);
             Statement stmt = conn.createStatement()) {
            
            stmt.execute(insertSql);
            long duration = System.currentTimeMillis() - startTime;
            
            LOG.info("Insert completed in {}ms for {} rows", duration, totalRows.get());
        }
    }
    
    private void executeSQL(String sql) throws IOException {
        try (Connection conn = GreenplumUtil.getConnection(config);
             Statement stmt = conn.createStatement()) {
            
            stmt.execute(sql);
        } catch (Exception e) {
            throw new IOException("Failed to execute SQL: " + sql, e);
        }
    }
    
    private void checkError() throws IOException {
        if (lastError != null) {
            throw new IOException("Writer thread encountered an error", lastError);
        }
    }
    
    private class WriterDataTransferService implements DataTransferService {
        @Override
        public ByteBuffer read(long timeout) {
            try {
                return dataQueue.poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        
        @Override
        public void write(byte[] data) {
            // Not used in sink writer
        }
        
        @Override
        public void recycleBuffer(ByteBuffer buffer) {
            // Buffer recycling not needed for simple implementation
        }
        
        @Override
        public void flush() {
            // Flush handled by main writer logic
        }
    }
}