package org.apache.seatunnel.connectors.greenplum.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GreenplumSourceReader implements SourceReader<SeaTunnelRow, GreenplumSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(GreenplumSourceReader.class);
    
    private final GreenplumConfig config;
    private final CatalogTable catalogTable;
    private final Context context;
    private final BlockingQueue<SeaTunnelRow> rowQueue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    
    private GpfdistServer gpfdistServer;
    private DataTransferService dataService;
    private volatile boolean noMoreSplits = false;
    private final Set<GreenplumSourceSplit> currentSplits = Collections.synchronizedSet(new HashSet<>());
    
    public GreenplumSourceReader(GreenplumConfig config, CatalogTable catalogTable, Context context) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.context = context;
        this.rowQueue = new LinkedBlockingQueue<>(config.getQueueSize());
    }
    
    @Override
    public void open() throws Exception {
        LOG.info("Opening Greenplum source reader");
        
        // Initialize data transfer service
        this.dataService = new ReaderDataTransferService();
        
        // Start GPFDIST server
        this.gpfdistServer = new GpfdistServer(0, dataService);
        this.gpfdistServer.start();
        
        LOG.info("GPFDIST server started on port: {}", gpfdistServer.getActualPort());
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Closing Greenplum source reader");
        running.set(false);
        
        if (gpfdistServer != null) {
            gpfdistServer.stop();
        }
    }
    
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        // Process current splits
        for (GreenplumSourceSplit split : currentSplits) {
            if (!running.get()) break;
            processSplit(split, output);
        }
        
        // Check if we're done
        if (noMoreSplits && currentSplits.isEmpty()) {
            context.signalNoMoreElement();
        }
    }
    
    @Override
    public List<GreenplumSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(currentSplits);
    }
    
    @Override
    public void addSplits(List<GreenplumSourceSplit> splits) {
        currentSplits.addAll(splits);
        LOG.info("Added {} splits to reader", splits.size());
    }
    
    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
        LOG.info("No more splits available");
    }
    
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Handle checkpoint completion
    }
    
    private void processSplit(GreenplumSourceSplit split, Collector<SeaTunnelRow> output) throws Exception {
        LOG.info("Processing split: {}", split.splitId());
        
        try (Connection connection = GreenplumUtil.getConnection(config)) {
            // Create external table for this split
            String externalTable = createExternalTable(connection, split);
            
            // Execute query through external table
            String query = buildQuery(externalTable, split);
            
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                while (rs.next() && running.get()) {
                    SeaTunnelRow row = GreenplumUtil.resultSetToRow(rs, catalogTable.getTableSchema());
                    output.collect(row);
                }
            } finally {
                // Drop external table
                dropExternalTable(connection, externalTable);
            }
        }
        
        currentSplits.remove(split);
    }
    
    private String createExternalTable(Connection connection, GreenplumSourceSplit split) throws SQLException {
        String externalTableName = "gpfdist_source_" + System.currentTimeMillis() + "_" + split.splitId();
        
        String createSql = String.format(
            "CREATE EXTERNAL TABLE %s (LIKE %s) " +
            "LOCATION ('gpfdist://%s:%d/%s') " +
            "FORMAT 'TEXT' (DELIMITER '%s' NULL '%s')",
            externalTableName,
            config.getTableName(),
            GreenplumUtil.getLocalHost(),
            gpfdistServer.getActualPort(),
            split.splitId(),
            config.getDelimiter(),
            config.getNullString()
        );
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createSql);
        }
        
        return externalTableName;
    }
    
    private String buildQuery(String externalTable, GreenplumSourceSplit split) {
        if (split.getPartitionStart() != null && split.getPartitionEnd() != null) {
            return String.format(
                "SELECT * FROM %s WHERE %s >= %s AND %s < %s",
                config.getTableName(),
                config.getPartitionColumn(),
                split.getPartitionStart(),
                config.getPartitionColumn(),
                split.getPartitionEnd()
            );
        } else if (config.getQuery() != null) {
            return config.getQuery();
        } else {
            return String.format("SELECT * FROM %s", config.getTableName());
        }
    }
    
    private void dropExternalTable(Connection connection, String tableName) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP EXTERNAL TABLE IF EXISTS " + tableName);
        } catch (SQLException e) {
            LOG.warn("Failed to drop external table: {}", tableName, e);
        }
    }
    
    private class ReaderDataTransferService implements DataTransferService {
        @Override
        public ByteBuffer read(long timeout) {
            try {
                SeaTunnelRow row = rowQueue.poll(timeout, TimeUnit.MILLISECONDS);
                if (row != null) {
                    String rowStr = GreenplumUtil.rowToString(row, config.getDelimiter(), config.getNullString());
                    return ByteBuffer.wrap(rowStr.getBytes(StandardCharsets.UTF_8));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
        
        @Override
        public void write(byte[] data) {
            // Not used in reader
        }
        
        @Override
        public void recycleBuffer(ByteBuffer buffer) {
            buffer.clear();
        }
        
        @Override
        public void flush() {
            // Not used in reader
        }
    }
}