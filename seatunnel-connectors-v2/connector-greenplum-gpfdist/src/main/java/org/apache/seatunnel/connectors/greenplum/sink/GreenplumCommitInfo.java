package org.apache.seatunnel.connectors.greenplum.sink;

import java.io.Serializable;

public class GreenplumCommitInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String writerId;
    private final String externalTableName;
    private final long rowCount;
    private final long timestamp;
    
    public GreenplumCommitInfo(String writerId, String externalTableName, long rowCount, long timestamp) {
        this.writerId = writerId;
        this.externalTableName = externalTableName;
        this.rowCount = rowCount;
        this.timestamp = timestamp;
    }
    
    public String getWriterId() {
        return writerId;
    }
    
    public String getExternalTableName() {
        return externalTableName;
    }
    
    public long getRowCount() {
        return rowCount;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public String toString() {
        return "GreenplumCommitInfo{" +
                "writerId='" + writerId + '\'' +
                ", externalTableName='" + externalTableName + '\'' +
                ", rowCount=" + rowCount +
                ", timestamp=" + timestamp +
                '}';
    }
}