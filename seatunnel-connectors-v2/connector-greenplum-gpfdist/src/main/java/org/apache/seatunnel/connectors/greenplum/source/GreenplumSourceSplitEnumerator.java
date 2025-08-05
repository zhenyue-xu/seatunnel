package org.apache.seatunnel.connectors.greenplum.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.greenplum.config.GreenplumConfig;
import org.apache.seatunnel.connectors.greenplum.util.GreenplumUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class GreenplumSourceSplitEnumerator implements SourceSplitEnumerator<GreenplumSourceSplit, GreenplumSourceState> {
    private static final Logger LOG = LoggerFactory.getLogger(GreenplumSourceSplitEnumerator.class);
    
    private final GreenplumConfig config;
    private final CatalogTable catalogTable;
    private final Context<GreenplumSourceSplit> context;
    private final Set<GreenplumSourceSplit> pendingSplits;
    private final Set<GreenplumSourceSplit> assignedSplits;
    
    public GreenplumSourceSplitEnumerator(
            GreenplumConfig config,
            CatalogTable catalogTable,
            Context<GreenplumSourceSplit> context,
            GreenplumSourceState state) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.context = context;
        this.pendingSplits = new HashSet<>();
        this.assignedSplits = new HashSet<>();
        
        if (state != null) {
            this.pendingSplits.addAll(state.getPendingSplits());
            this.assignedSplits.addAll(state.getAssignedSplits());
        }
    }
    
    @Override
    public void open() {
        LOG.info("Opening Greenplum source split enumerator");
        
        if (pendingSplits.isEmpty() && assignedSplits.isEmpty()) {
            discoverSplits();
        }
    }
    
    @Override
    public void run() {
        // In batch mode, we discover all splits at once
        if (pendingSplits.isEmpty() && assignedSplits.isEmpty()) {
            discoverSplits();
        }
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Closing Greenplum source split enumerator");
    }
    
    @Override
    public void addSplitsBack(List<GreenplumSourceSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        assignedSplits.removeAll(splits);
        pendingSplits.addAll(splits);
    }
    
    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }
    
    @Override
    public void registerReader(int subtaskId) {
        LOG.info("Registering reader for subtask {}", subtaskId);
        assignSplits();
    }
    
    @Override
    public GreenplumSourceState snapshotState(long checkpointId) {
        return new GreenplumSourceState(
            new ArrayList<>(pendingSplits),
            new ArrayList<>(assignedSplits)
        );
    }
    
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Handle checkpoint completion
    }
    
    @Override
    public void handleSplitRequest(int subtaskId) {
        // Handle split request from reader
        assignSplits();
    }
    
    private void discoverSplits() {
        LOG.info("Discovering splits for table: {}", config.getTableName());
        
        try (Connection connection = GreenplumUtil.getConnection(config)) {
            if (config.getPartitionColumn() != null) {
                discoverPartitionedSplits(connection);
            } else {
                discoverNonPartitionedSplits();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to discover splits", e);
        }
        
        LOG.info("Discovered {} splits", pendingSplits.size());
    }
    
    private void discoverPartitionedSplits(Connection connection) throws Exception {
        String minMaxQuery = String.format(
            "SELECT MIN(%s) as min_val, MAX(%s) as max_val FROM %s",
            config.getPartitionColumn(),
            config.getPartitionColumn(),
            config.getTableName()
        );
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(minMaxQuery)) {
            
            if (rs.next()) {
                long minVal = config.getPartitionLowerBound() != null ? 
                    config.getPartitionLowerBound() : rs.getLong("min_val");
                long maxVal = config.getPartitionUpperBound() != null ? 
                    config.getPartitionUpperBound() : rs.getLong("max_val");
                
                int numPartitions = config.getParallelism();
                long range = maxVal - minVal;
                long partitionSize = range / numPartitions;
                
                for (int i = 0; i < numPartitions; i++) {
                    long start = minVal + (i * partitionSize);
                    long end = (i == numPartitions - 1) ? maxVal + 1 : start + partitionSize;
                    
                    String splitId = String.format("split_%d", i);
                    GreenplumSourceSplit split = new GreenplumSourceSplit(
                        splitId,
                        config.getTableName(),
                        config.getPartitionColumn(),
                        start,
                        end
                    );
                    
                    pendingSplits.add(split);
                }
            }
        }
    }
    
    private void discoverNonPartitionedSplits() {
        // For non-partitioned tables, create splits based on parallelism
        for (int i = 0; i < config.getParallelism(); i++) {
            String splitId = String.format("split_%d", i);
            GreenplumSourceSplit split = new GreenplumSourceSplit(splitId, config.getTableName());
            pendingSplits.add(split);
        }
    }
    
    private void assignSplits() {
        if (pendingSplits.isEmpty()) {
            return;
        }
        
        Map<Integer, List<GreenplumSourceSplit>> splitAssignment = new HashMap<>();
        
        for (int subtaskId : context.registeredReaders()) {
            if (pendingSplits.isEmpty()) {
                break;
            }
            
            GreenplumSourceSplit split = pendingSplits.iterator().next();
            pendingSplits.remove(split);
            assignedSplits.add(split);
            
            splitAssignment.computeIfAbsent(subtaskId, k -> new ArrayList<>()).add(split);
        }
        
        splitAssignment.forEach(context::assignSplit);
        
        // Signal no more splits if all splits have been assigned
        if (pendingSplits.isEmpty()) {
            context.registeredReaders().forEach(context::signalNoMoreSplits);
        }
    }
}