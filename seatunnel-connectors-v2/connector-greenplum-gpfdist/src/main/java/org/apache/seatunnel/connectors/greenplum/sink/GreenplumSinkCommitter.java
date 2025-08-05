package org.apache.seatunnel.connectors.greenplum.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.connectors.greenplum.config.GreenplumConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GreenplumSinkCommitter implements SinkAggregatedCommitter<GreenplumCommitInfo, GreenplumAggregatedCommitInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(GreenplumSinkCommitter.class);
    
    private final GreenplumConfig config;
    
    public GreenplumSinkCommitter(GreenplumConfig config) {
        this.config = config;
    }
    
    @Override
    public List<GreenplumAggregatedCommitInfo> commit(
            List<GreenplumAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        
        List<GreenplumAggregatedCommitInfo> failedCommits = new ArrayList<>();
        
        for (GreenplumAggregatedCommitInfo commitInfo : aggregatedCommitInfo) {
            try {
                LOG.info("Committing transaction: {} with {} total rows from {} writers", 
                        commitInfo.getTransactionId(),
                        commitInfo.getTotalRows(),
                        commitInfo.getCommitInfos().size());
                
                // In GPFDIST mode, the actual commit happens in the writer
                // This is mainly for logging and monitoring
                
                for (GreenplumCommitInfo info : commitInfo.getCommitInfos()) {
                    LOG.info("Writer {} committed {} rows", 
                            info.getWriterId(), info.getRowCount());
                }
                
            } catch (Exception e) {
                LOG.error("Failed to commit transaction: " + commitInfo.getTransactionId(), e);
                failedCommits.add(commitInfo);
            }
        }
        
        return failedCommits;
    }
    
    @Override
    public void abort(List<GreenplumAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        for (GreenplumAggregatedCommitInfo commitInfo : aggregatedCommitInfo) {
            LOG.warn("Aborting transaction: {}", commitInfo.getTransactionId());
        }
    }
    
    @Override
    public GreenplumAggregatedCommitInfo combine(List<GreenplumCommitInfo> commitInfos) {
        Map<String, List<GreenplumCommitInfo>> groupedByTransaction = new HashMap<>();
        
        for (GreenplumCommitInfo info : commitInfos) {
            String transactionId = "txn_" + info.getTimestamp();
            groupedByTransaction.computeIfAbsent(transactionId, k -> new ArrayList<>()).add(info);
        }
        
        // For simplicity, we'll create one aggregated commit info
        String transactionId = "txn_" + System.currentTimeMillis();
        long totalRows = commitInfos.stream().mapToLong(GreenplumCommitInfo::getRowCount).sum();
        
        return new GreenplumAggregatedCommitInfo(transactionId, commitInfos, totalRows);
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Closing Greenplum sink committer");
    }
}