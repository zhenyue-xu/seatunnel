package org.apache.seatunnel.connectors.greenplum.sink;

import java.io.Serializable;
import java.util.List;

public class GreenplumAggregatedCommitInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String transactionId;
    private final List<GreenplumCommitInfo> commitInfos;
    private final long totalRows;
    
    public GreenplumAggregatedCommitInfo(
            String transactionId, 
            List<GreenplumCommitInfo> commitInfos,
            long totalRows) {
        this.transactionId = transactionId;
        this.commitInfos = commitInfos;
        this.totalRows = totalRows;
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public List<GreenplumCommitInfo> getCommitInfos() {
        return commitInfos;
    }
    
    public long getTotalRows() {
        return totalRows;
    }
}