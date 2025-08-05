package org.apache.seatunnel.connectors.greenplum.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProgressTracker {
    private static final Logger LOG = LoggerFactory.getLogger(ProgressTracker.class);
    
    private final Map<String, Long> processingMs = new ConcurrentHashMap<>();
    
    /**
     * Track the execution time of a code block
     */
    public <T> T trackProgress(String metricKey, ProgressCallable<T> callable) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            return callable.call();
        } finally {
            long endTime = System.currentTimeMillis();
            long timeTaken = Math.max(endTime - startTime, 0);
            
            processingMs.merge(metricKey, timeTaken, Long::sum);
            LOG.debug("{} took {} ms", metricKey, timeTaken);
        }
    }
    
    /**
     * Track the execution time of a code block that doesn't throw checked exceptions
     */
    public <T> T trackProgressRuntime(String metricKey, ProgressRunnable<T> runnable) {
        long startTime = System.currentTimeMillis();
        try {
            return runnable.run();
        } finally {
            long endTime = System.currentTimeMillis();
            long timeTaken = Math.max(endTime - startTime, 0);
            
            processingMs.merge(metricKey, timeTaken, Long::sum);
            LOG.debug("{} took {} ms", metricKey, timeTaken);
        }
    }
    
    /**
     * Get all tracked metrics
     */
    public Map<String, Long> getResults() {
        return new ConcurrentHashMap<>(processingMs);
    }
    
    /**
     * Get a specific metric value
     */
    public long getMetric(String metricKey) {
        return processingMs.getOrDefault(metricKey, 0L);
    }
    
    /**
     * Reset all metrics
     */
    public void reset() {
        processingMs.clear();
    }
    
    /**
     * Generate a report of all tracked metrics
     */
    public String reportTimeTaken() {
        if (processingMs.isEmpty()) {
            return "No metrics tracked";
        }
        
        StringBuilder report = new StringBuilder();
        processingMs.forEach((key, value) -> {
            if (report.length() > 0) {
                report.append(", ");
            }
            report.append(key).append("=").append(value).append("ms");
        });
        
        return report.toString();
    }
    
    /**
     * Functional interface for code blocks that may throw checked exceptions
     */
    @FunctionalInterface
    public interface ProgressCallable<T> {
        T call() throws Exception;
    }
    
    /**
     * Functional interface for code blocks that only throw runtime exceptions
     */
    @FunctionalInterface
    public interface ProgressRunnable<T> {
        T run();
    }
}