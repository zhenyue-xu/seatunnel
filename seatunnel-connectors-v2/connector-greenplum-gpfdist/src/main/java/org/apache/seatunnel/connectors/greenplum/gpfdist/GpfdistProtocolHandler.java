package org.apache.seatunnel.connectors.greenplum.gpfdist;

import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class GpfdistProtocolHandler {
    private static final Logger LOG = LoggerFactory.getLogger(GpfdistProtocolHandler.class);
    
    private final DataTransferService dataService;
    private final ReentrantLock lock = new ReentrantLock();
    
    // Metrics
    private final AtomicLong totalBytes = new AtomicLong(0);
    private final AtomicLong processingMs = new AtomicLong(0);
    private final AtomicInteger segmentCount = new AtomicInteger(0);
    private final AtomicInteger segmentDoneCount = new AtomicInteger(0);
    
    // Segment management for POST requests
    private final Map<String, byte[]> segmentQueues = new ConcurrentHashMap<>();
    private final Map<String, Integer> segmentPass = new ConcurrentHashMap<>();
    
    public GpfdistProtocolHandler(DataTransferService dataService) {
        this.dataService = dataService;
    }
    
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod().toLowerCase();
        
        if ("get".equals(method)) {
            handleGetRequest(exchange);
        } else if ("post".equals(method)) {
            handlePostRequest(exchange);
        } else {
            exchange.sendResponseHeaders(405, -1);
        }
    }
    
    private void handleGetRequest(HttpExchange exchange) throws IOException {
        Map<String, List<String>> headers = exchange.getRequestHeaders();
        String segmentId = getHeaderValue(headers, "X-gp-segment-id");
        
        LOG.debug("GET request from segment: {}", segmentId);
        
        // Set response headers
        setCommonResponseHeaders(exchange);
        exchange.getResponseHeaders().put("X-GP-PROTO", List.of("1"));
        exchange.sendResponseHeaders(200, 0);
        
        long startTime = System.currentTimeMillis();
        long nBytes = 0;
        int nChunks = 0;
        
        try (OutputStream os = exchange.getResponseBody()) {
            ByteBuffer buffer;
            while ((buffer = dataService.read(10000)) != null) {
                if (buffer.remaining() > 0) {
                    nChunks++;
                    int size = buffer.remaining();
                    byte[] data = new byte[size];
                    buffer.get(data);
                    os.write(data);
                    nBytes += size;
                    
                    dataService.recycleBuffer(buffer);
                }
            }
            os.flush();
        }
        
        long duration = System.currentTimeMillis() - startTime;
        processingMs.addAndGet(duration);
        totalBytes.addAndGet(nBytes);
        
        LOG.info("GET for segment {} complete in {}ms, bytes: {}, chunks: {}", 
                segmentId, duration, nBytes, nChunks);
    }
    
    private void handlePostRequest(HttpExchange exchange) throws IOException {
        lock.lock();
        try {
            Map<String, List<String>> headers = exchange.getRequestHeaders();
            String segmentId = getHeaderValue(headers, "X-gp-segment-id");
            int gpSeq = Integer.parseInt(getHeaderValue(headers, "X-gp-seq", "0"));
            int contentLength = Integer.parseInt(getHeaderValue(headers, "Content-length", "0"));
            int segmentDone = Integer.parseInt(getHeaderValue(headers, "X-gp-done", "0"));
            
            LOG.debug("POST request from segment: {}, seq: {}, length: {}, done: {}", 
                     segmentId, gpSeq, contentLength, segmentDone);
            
            if (gpSeq == 1) {
                segmentCount.incrementAndGet();
            }
            
            // Track segment passes
            int thisSegPass = segmentPass.compute(segmentId, (k, v) -> v == null ? 1 : v + 1);
            
            // Initialize segment queue
            segmentQueues.putIfAbsent(segmentId, new byte[0]);
            
            // Read request data
            long startTime = System.currentTimeMillis();
            byte[] inputData = readRequestData(exchange.getRequestBody(), contentLength);
            
            // Process data
            processSegmentData(segmentId, inputData, segmentDone);
            
            if (segmentDone != 0) {
                segmentDoneCount.incrementAndGet();
                dataService.flush();
                segmentQueues.remove(segmentId);
                LOG.info("Segment {} completed after {} passes", segmentId, thisSegPass);
            }
            
            // Send response
            setCommonResponseHeaders(exchange);
            exchange.getResponseHeaders().put("X-GP-PROTO", List.of("0"));
            exchange.sendResponseHeaders(200, 0);
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.flush();
            }
            
            long duration = System.currentTimeMillis() - startTime;
            processingMs.addAndGet(duration);
            totalBytes.addAndGet(inputData.length);
            
            LOG.debug("POST for segment {} pass {} complete in {}ms, bytes: {}", 
                     segmentId, thisSegPass, duration, inputData.length);
            
        } finally {
            lock.unlock();
        }
    }
    
    private void processSegmentData(String segmentId, byte[] data, int segmentDone) {
        byte[] existingData = segmentQueues.get(segmentId);
        byte[] combinedData = new byte[existingData.length + data.length];
        
        System.arraycopy(existingData, 0, combinedData, 0, existingData.length);
        System.arraycopy(data, 0, combinedData, existingData.length, data.length);
        
        // Find last newline
        int lastNewlineIndex = -1;
        for (int i = combinedData.length - 1; i >= 0; i--) {
            if (combinedData[i] == '\n') {
                lastNewlineIndex = i;
                break;
            }
        }
        
        if (lastNewlineIndex >= 0) {
            // Send complete lines
            byte[] completeData = new byte[lastNewlineIndex + 1];
            System.arraycopy(combinedData, 0, completeData, 0, lastNewlineIndex + 1);
            dataService.write(completeData);
            
            // Save remaining data
            byte[] remainingData = new byte[combinedData.length - lastNewlineIndex - 1];
            System.arraycopy(combinedData, lastNewlineIndex + 1, remainingData, 0, remainingData.length);
            segmentQueues.put(segmentId, remainingData);
        } else {
            // No complete lines, save all data
            segmentQueues.put(segmentId, combinedData);
        }
        
        // Handle final segment data
        if (segmentDone != 0 && segmentQueues.get(segmentId).length > 0) {
            LOG.warn("Segment {} has remaining data: {} bytes", 
                    segmentId, segmentQueues.get(segmentId).length);
        }
    }
    
    private byte[] readRequestData(InputStream is, int contentLength) throws IOException {
        byte[] buffer = new byte[contentLength];
        int totalRead = 0;
        
        while (totalRead < contentLength) {
            int read = is.read(buffer, totalRead, contentLength - totalRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream");
            }
            totalRead += read;
        }
        
        return buffer;
    }
    
    private void setCommonResponseHeaders(HttpExchange exchange) {
        Map<String, List<String>> responseHeaders = exchange.getResponseHeaders();
        responseHeaders.put("Content-type", List.of("text/plain"));
        responseHeaders.put("X-GPFDIST-VERSION", List.of("SeaTunnel-GP-1.0"));
        responseHeaders.put("Cache-Control", List.of("no-cache"));
        responseHeaders.put("Connection", List.of("close"));
        responseHeaders.put("Expires", List.of("0"));
    }
    
    private String getHeaderValue(Map<String, List<String>> headers, String key) {
        return getHeaderValue(headers, key, null);
    }
    
    private String getHeaderValue(Map<String, List<String>> headers, String key, String defaultValue) {
        List<String> values = headers.get(key);
        if (values != null && !values.isEmpty()) {
            return values.get(0);
        }
        if (defaultValue != null) {
            return defaultValue;
        }
        throw new IllegalArgumentException("Missing required header: " + key);
    }
    
    public GpfdistMetrics getMetrics() {
        return new GpfdistMetrics(
            totalBytes.get(),
            processingMs.get(),
            segmentCount.get(),
            segmentDoneCount.get()
        );
    }
    
    public static class GpfdistMetrics {
        public final long totalBytes;
        public final long processingMs;
        public final int segmentCount;
        public final int segmentDoneCount;
        
        public GpfdistMetrics(long totalBytes, long processingMs, int segmentCount, int segmentDoneCount) {
            this.totalBytes = totalBytes;
            this.processingMs = processingMs;
            this.segmentCount = segmentCount;
            this.segmentDoneCount = segmentDoneCount;
        }
    }
}