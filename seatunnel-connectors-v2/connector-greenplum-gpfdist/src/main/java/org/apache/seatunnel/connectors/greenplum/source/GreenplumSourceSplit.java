package org.apache.seatunnel.connectors.greenplum.source;

import org.apache.seatunnel.api.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

public class GreenplumSourceSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String splitId;
    private final String tableName;
    private final String partitionColumn;
    private final Long partitionStart;
    private final Long partitionEnd;
    
    public GreenplumSourceSplit(String splitId, String tableName) {
        this(splitId, tableName, null, null, null);
    }
    
    public GreenplumSourceSplit(
            String splitId, 
            String tableName, 
            String partitionColumn, 
            Long partitionStart, 
            Long partitionEnd) {
        this.splitId = splitId;
        this.tableName = tableName;
        this.partitionColumn = partitionColumn;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
    }
    
    @Override
    public String splitId() {
        return splitId;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public String getPartitionColumn() {
        return partitionColumn;
    }
    
    public Long getPartitionStart() {
        return partitionStart;
    }
    
    public Long getPartitionEnd() {
        return partitionEnd;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GreenplumSourceSplit that = (GreenplumSourceSplit) o;
        return Objects.equals(splitId, that.splitId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
    
    @Override
    public String toString() {
        return "GreenplumSourceSplit{" +
                "splitId='" + splitId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", partitionColumn='" + partitionColumn + '\'' +
                ", partitionStart=" + partitionStart +
                ", partitionEnd=" + partitionEnd +
                '}';
    }
}