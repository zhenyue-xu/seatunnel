package org.apache.seatunnel.connectors.greenplum.gpfdist;

import java.nio.ByteBuffer;

/**
 * Interface for data transfer between GPFDIST server and SeaTunnel components
 */
public interface DataTransferService {
    /**
     * Read data with timeout
     * @param timeout timeout in milliseconds
     * @return ByteBuffer containing data or null if no data available
     */
    ByteBuffer read(long timeout);
    
    /**
     * Write data
     * @param data data to write
     */
    void write(byte[] data);
    
    /**
     * Recycle buffer after use
     * @param buffer buffer to recycle
     */
    void recycleBuffer(ByteBuffer buffer);
    
    /**
     * Flush any pending data
     */
    void flush();
}