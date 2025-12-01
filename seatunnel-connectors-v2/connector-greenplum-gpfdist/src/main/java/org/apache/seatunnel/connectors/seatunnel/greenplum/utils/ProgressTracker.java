/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.greenplum.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ProgressTracker {

    private final Map<String, AtomicLong> timings = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> bytes = new ConcurrentHashMap<>();
    private final long creationTime = System.currentTimeMillis();

    public <T> T trackProgress(String operation, ProgressSupplier<T> supplier) {
        long startTime = System.currentTimeMillis();
        try {
            T result = supplier.get();
            return result;
        } catch (Exception e) {
            log.warn(
                    "Operation {} failed after {} ms",
                    operation,
                    System.currentTimeMillis() - startTime,
                    e);
            throw new RuntimeException("Operation " + operation + " failed", e);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            timings.computeIfAbsent(operation, k -> new AtomicLong(0)).addAndGet(duration);
            counts.computeIfAbsent(operation, k -> new AtomicLong(0)).incrementAndGet();

            if (duration > 1000) {
                log.debug("Operation {} took {} ms", operation, duration);
            }
        }
    }

    public void trackProgress(String operation, ProgressRunnable runnable) {
        trackProgress(
                operation,
                () -> {
                    runnable.run();
                    return null;
                });
    }

    public void trackBytes(String operation, long byteCount) {
        bytes.computeIfAbsent(operation, k -> new AtomicLong(0)).addAndGet(byteCount);
    }

    public void incrementCount(String operation) {
        incrementCount(operation, 1);
    }

    public void incrementCount(String operation, long count) {
        counts.computeIfAbsent(operation, k -> new AtomicLong(0)).addAndGet(count);
    }

    public long getTotalTime(String operation) {
        AtomicLong time = timings.get(operation);
        return time != null ? time.get() : 0;
    }

    public long getCount(String operation) {
        AtomicLong count = counts.get(operation);
        return count != null ? count.get() : 0;
    }

    public long getTotalBytes(String operation) {
        AtomicLong byteCount = bytes.get(operation);
        return byteCount != null ? byteCount.get() : 0;
    }

    public double getAverageTime(String operation) {
        long total = getTotalTime(operation);
        long count = getCount(operation);
        return count > 0 ? (double) total / count : 0.0;
    }

    public String reportTimeTaken() {
        if (timings.isEmpty()) {
            return "No operations tracked";
        }

        StringBuilder report = new StringBuilder();
        long totalRuntime = System.currentTimeMillis() - creationTime;

        report.append("Performance Report (total runtime: ").append(totalRuntime).append("ms):\n");

        timings.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue().get(), e1.getValue().get()))
                .forEach(
                        entry -> {
                            String operation = entry.getKey();
                            long totalTime = entry.getValue().get();
                            long count = getCount(operation);
                            long totalBytes = getTotalBytes(operation);

                            if (report.length() > 0 && !report.toString().endsWith("\n")) {
                                report.append(", ");
                            }

                            report.append(operation).append("=").append(totalTime).append("ms");

                            if (count > 1) {
                                report.append("(")
                                        .append(count)
                                        .append("x, avg=")
                                        .append(String.format("%.1f", getAverageTime(operation)))
                                        .append("ms)");
                            }

                            if (totalBytes > 0) {
                                report.append("[").append(formatBytes(totalBytes)).append("]");
                            }
                        });

        return report.toString();
    }

    public String reportSimple() {
        if (timings.isEmpty()) {
            return "";
        }

        StringBuilder report = new StringBuilder();
        timings.forEach(
                (operation, time) -> {
                    if (report.length() > 0) {
                        report.append(", ");
                    }
                    report.append(operation).append("=").append(time.get()).append("ms");
                });
        return report.toString();
    }

    public Map<String, Long> getTimeResults() {
        Map<String, Long> results = new ConcurrentHashMap<>();
        timings.forEach((k, v) -> results.put(k, v.get()));
        return results;
    }

    public Map<String, Long> getCountResults() {
        Map<String, Long> results = new ConcurrentHashMap<>();
        counts.forEach((k, v) -> results.put(k, v.get()));
        return results;
    }

    public Map<String, Long> getBytesResults() {
        Map<String, Long> results = new ConcurrentHashMap<>();
        bytes.forEach((k, v) -> results.put(k, v.get()));
        return results;
    }

    public void reset() {
        timings.clear();
        counts.clear();
        bytes.clear();
    }

    public void merge(ProgressTracker other) {
        other.timings.forEach(
                (k, v) -> timings.computeIfAbsent(k, key -> new AtomicLong(0)).addAndGet(v.get()));
        other.counts.forEach(
                (k, v) -> counts.computeIfAbsent(k, key -> new AtomicLong(0)).addAndGet(v.get()));
        other.bytes.forEach(
                (k, v) -> bytes.computeIfAbsent(k, key -> new AtomicLong(0)).addAndGet(v.get()));
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return String.format("%.1fKB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1fMB", bytes / (1024.0 * 1024));
        return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
    }

    @FunctionalInterface
    public interface ProgressSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    public interface ProgressRunnable {
        void run() throws Exception;
    }
}
