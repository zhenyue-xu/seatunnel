package org.apache.seatunnel.connectors.greenplum.source;

import java.io.Serializable;
import java.util.List;

public class GreenplumSourceState implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final List<GreenplumSourceSplit> pendingSplits;
    private final List<GreenplumSourceSplit> assignedSplits;
    
    public GreenplumSourceState(
            List<GreenplumSourceSplit> pendingSplits,
            List<GreenplumSourceSplit> assignedSplits) {
        this.pendingSplits = pendingSplits;
        this.assignedSplits = assignedSplits;
    }
    
    public List<GreenplumSourceSplit> getPendingSplits() {
        return pendingSplits;
    }
    
    public List<GreenplumSourceSplit> getAssignedSplits() {
        return assignedSplits;
    }
}