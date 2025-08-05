package org.apache.seatunnel.connectors.greenplum.sink;

import java.io.Serializable;

public class GreenplumSinkState implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String writerId;
    private final long processedRows;
    private final long checkpointId;
    
    public GreenplumSinkState(String writerId, long processedRows, long checkpointId) {
        this.writerId = writerId;
        this.processedRows = processedRows;
        this.checkpointId = checkpointId;
    }
    
    public String getWriterId() {
        return writerId;
    }
    
    public long getProcessedRows() {
        return processedRows;
    }
    
    public long getCheckpointId() {
        return checkpointId;
    }
}