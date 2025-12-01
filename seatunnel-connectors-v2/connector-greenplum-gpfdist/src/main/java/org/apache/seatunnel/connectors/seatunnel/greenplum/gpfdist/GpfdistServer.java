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

package org.apache.seatunnel.connectors.seatunnel.greenplum.gpfdist;

import org.apache.seatunnel.connectors.seatunnel.greenplum.utils.NetworkUtils;
import org.apache.seatunnel.connectors.seatunnel.greenplum.utils.ProgressTracker;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class GpfdistServer {

    private final int port;
    private final String bindAddress;
    private final String externalAddress;
    private final BufferExchange bufferExchange;

    private ServerSocket serverSocket;
    private ThreadPoolExecutor executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private final AtomicLong totalBytes = new AtomicLong(0);
    private volatile boolean transferComplete = false;
    private final ProgressTracker progressTracker = new ProgressTracker();

    private final Map<String, SegmentInfo> activeSegments = new ConcurrentHashMap<>();
    private final AtomicInteger completedSegments = new AtomicInteger(0);
    private volatile int expectedSegmentCount = -1;

    private static final String HTTP_OK_RESPONSE =
            "HTTP/1.1 200 OK\r\n"
                    + "Content-Type: text/plain\r\n"
                    + "X-GPFDIST-VERSION: 2.0.0 build commit:seatunnel\r\n"
                    + "X-GP-PROTO: 1\r\n"
                    + "X-GPFDIST-VERSION: 6.8.1\r\n"
                    + "Cache-Control: no-cache\r\n"
                    + "Expires: 0\r\n"
                    + "Connection: close\r\n\r\n";

    private static final String HTTP_ERROR_RESPONSE_TEMPLATE =
            "HTTP/1.1 %d %s\r\n" + "Content-Type: text/plain\r\n" + "Connection: close\r\n\r\n";

    public GpfdistServer(
            int port, String bindAddress, String externalAddress, BufferExchange bufferExchange) {
        this.port = port;
        this.bindAddress = bindAddress != null ? bindAddress : "0.0.0.0";
        this.bufferExchange = bufferExchange;

        if (externalAddress != null) {
            this.externalAddress = externalAddress;
        } else {
            NetworkUtils.HostInfo hostInfo = NetworkUtils.getInstance().getLocalHostNameAndIp();
            this.externalAddress = hostInfo.getIpAddress();
        }

        log.debug(
                "GpfdistServer configured: port={}, bind={}, external={}",
                port,
                this.bindAddress,
                this.externalAddress);
    }

    public void start() throws IOException {
        if (running.compareAndSet(false, true)) {
            try {
                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.setSoTimeout(1000);
                serverSocket.bind(new InetSocketAddress(bindAddress, port));

                executor =
                        new ThreadPoolExecutor(
                                2,
                                20,
                                60L,
                                TimeUnit.SECONDS,
                                new LinkedBlockingQueue<>(100),
                                r -> {
                                    Thread t = new Thread(r, "gpfdist-worker");
                                    t.setDaemon(true);
                                    return t;
                                });

                CompletableFuture.runAsync(this::acceptConnections, executor);

                log.info(
                        "GPfdist server started on {}:{}, external address: {}",
                        bindAddress,
                        getPort(),
                        externalAddress);
            } catch (IOException e) {
                running.set(false);
                throw e;
            }
        }
    }

    private void acceptConnections() {
        while (running.get() && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                connectionCount.incrementAndGet();

                log.debug("Accepted connection from: {}", clientSocket.getRemoteSocketAddress());

                executor.submit(() -> handleConnection(clientSocket));

            } catch (SocketTimeoutException e) {

            } catch (IOException e) {
                if (running.get()) {
                    log.warn("Error accepting connection", e);
                }
            }
        }
    }

    private void handleConnection(Socket clientSocket) {
        try {
            clientSocket.setSoTimeout(30000);

            try (BufferedReader in =
                            new BufferedReader(
                                    new InputStreamReader(clientSocket.getInputStream()));
                    OutputStream out = clientSocket.getOutputStream()) {

                String requestLine = in.readLine();
                if (requestLine == null) {
                    return;
                }

                log.debug("Received request: {}", requestLine);

                String[] requestParts = requestLine.split(" ");
                if (requestParts.length < 3) {
                    sendErrorResponse(out, 400, "Bad Request");
                    return;
                }

                String method = requestParts[0];
                String path = requestParts[1];

                Map<String, String> headers = readHeaders(in);

                if ("GET".equals(method)) {
                    handleGetRequest(out, headers);
                } else if ("POST".equals(method)) {
                    handlePostRequest(in, out, headers);
                } else {
                    sendErrorResponse(out, 405, "Method Not Allowed");
                }
            }
        } catch (IOException e) {
            log.debug("Connection handling error: {}", e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                log.debug("Error closing client socket: {}", e.getMessage());
            }
            connectionCount.decrementAndGet();
        }
    }

    private Map<String, String> readHeaders(BufferedReader in) throws IOException {
        Map<String, String> headers = new HashMap<>();
        String line;

        while ((line = in.readLine()) != null && !line.isEmpty()) {
            String[] parts = line.split(":", 2);
            if (parts.length == 2) {
                headers.put(parts[0].trim(), parts[1].trim());
            }
        }

        return headers;
    }

    private void handleGetRequest(OutputStream out, Map<String, String> headers)
            throws IOException {
        String segmentId = headers.get("X-gp-segment-id");

        log.debug("Handling GET request for segment: {}", segmentId);

        out.write(HTTP_OK_RESPONSE.getBytes(StandardCharsets.UTF_8));

        long startTime = System.currentTimeMillis();
        long bytesTransferred = 0;
        int chunksTransferred = 0;

        try {

            while (!transferComplete || bufferExchange.getAvailableBuffers() > 0) {
                ByteBuffer buffer = bufferExchange.get(1000);

                if (buffer != null && buffer.hasRemaining()) {
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);

                    progressTracker.trackProgress(
                            "web_write",
                            () -> {
                                try {
                                    out.write(data);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });

                    bytesTransferred += data.length;
                    chunksTransferred++;

                    bufferExchange.recycleBuffer(buffer);

                    if (chunksTransferred % 10 == 0) {
                        out.flush();
                    }
                } else if (transferComplete && bufferExchange.isTransferComplete()) {

                    break;
                } else if (!transferComplete) {

                    Thread.sleep(10);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Error during GET request processing: {}", e.getMessage());
        } finally {
            try {
                out.flush();
            } catch (IOException e) {
                log.debug("Error flushing output stream: {}", e.getMessage());
            }
        }

        totalBytes.addAndGet(bytesTransferred);
        long duration = System.currentTimeMillis() - startTime;

        log.debug(
                "GET request completed for segment {}: {} bytes in {} chunks, took {}ms",
                segmentId,
                bytesTransferred,
                chunksTransferred,
                duration);
    }

    private void handlePostRequest(BufferedReader in, OutputStream out, Map<String, String> headers)
            throws IOException {
        String segmentId = headers.get("X-gp-segment-id");
        String contentLengthStr = headers.get("Content-Length");
        String gpSeq = headers.get("X-gp-seq");
        String gpDone = headers.get("X-gp-done");
        String gpSegmentCount = headers.get("X-gp-segment-count");

        log.debug(
                "Handling POST request: segment={}, seq={}, done={}, contentLength={}",
                segmentId,
                gpSeq,
                gpDone,
                contentLengthStr);

        if (gpSegmentCount != null && expectedSegmentCount == -1) {
            try {
                expectedSegmentCount = Integer.parseInt(gpSegmentCount);
                log.debug("Expected segment count: {}", expectedSegmentCount);
            } catch (NumberFormatException e) {
                log.warn("Invalid segment count: {}", gpSegmentCount);
            }
        }

        if (segmentId != null) {
            activeSegments.computeIfAbsent(segmentId, k -> new SegmentInfo(segmentId));
        }

        long bytesRead = 0;

        try {

            if (contentLengthStr != null) {
                int contentLength = Integer.parseInt(contentLengthStr);
                if (contentLength > 0) {
                    char[] buffer = new char[contentLength];
                    int totalRead = 0;

                    while (totalRead < contentLength) {
                        int read = in.read(buffer, totalRead, contentLength - totalRead);
                        if (read == -1) {
                            break;
                        }
                        totalRead += read;
                    }

                    if (totalRead > 0) {
                        String data = new String(buffer, 0, totalRead);
                        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
                        bufferExchange.put(dataBytes);
                        bytesRead = dataBytes.length;
                    }
                }
            } else {

                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = in.readLine()) != null) {
                    sb.append(line).append('\n');
                }

                if (sb.length() > 0) {
                    byte[] dataBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
                    bufferExchange.put(dataBytes);
                    bytesRead = dataBytes.length;
                }
            }

            if ("1".equals(gpDone)) {
                if (segmentId != null) {
                    SegmentInfo segInfo = activeSegments.get(segmentId);
                    if (segInfo != null && !segInfo.isCompleted()) {
                        segInfo.setCompleted(true);
                        int completed = completedSegments.incrementAndGet();

                        log.debug(
                                "Segment {} completed. Total completed: {}/{}",
                                segmentId,
                                completed,
                                expectedSegmentCount);

                        if (expectedSegmentCount > 0 && completed >= expectedSegmentCount) {
                            transferComplete = true;
                            log.info("All segments completed, transfer finished");
                        }
                    }
                }
            }

        } catch (NumberFormatException e) {
            log.warn("Invalid content length: {}", contentLengthStr);
            sendErrorResponse(out, 400, "Bad Request");
            return;
        } catch (Exception e) {
            log.error("Error processing POST request", e);
            sendErrorResponse(out, 500, "Internal Server Error");
            return;
        }

        out.write(HTTP_OK_RESPONSE.getBytes(StandardCharsets.UTF_8));
        out.flush();

        totalBytes.addAndGet(bytesRead);
        log.debug("POST request processed: {} bytes from segment {}", bytesRead, segmentId);
    }

    private void sendErrorResponse(OutputStream out, int statusCode, String reasonPhrase)
            throws IOException {
        String response = String.format(HTTP_ERROR_RESPONSE_TEMPLATE, statusCode, reasonPhrase);
        out.write(response.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public void addData(String data) {
        if (data != null && !data.isEmpty()) {
            addData(data.getBytes(StandardCharsets.UTF_8));
        }
    }

    public void addData(byte[] data) {
        if (data != null && data.length > 0) {
            bufferExchange.put(data);
        }
    }

    public void endTransfer() {
        transferComplete = true;
        bufferExchange.flush();
        log.info(
                "Transfer ended. Total connections: {}, total bytes: {}",
                connectionCount.get(),
                totalBytes.get());
    }

    public int getPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : port;
    }

    public String getUrl() {
        return String.format("gpfdist://%s:%d/data", externalAddress, getPort());
    }

    public boolean isHealthy() {
        return running.get() && serverSocket != null && !serverSocket.isClosed();
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            try {

                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }

                if (executor != null) {
                    executor.shutdown();
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                }

                activeSegments.clear();

                log.info(
                        "GPfdist server stopped. Final stats: connections={}, bytes={}, segments={}",
                        connectionCount.get(),
                        totalBytes.get(),
                        completedSegments.get());

            } catch (Exception e) {
                log.warn("Error stopping GPfdist server", e);
            }
        }
    }

    public String getProgressReport() {
        return progressTracker.reportTimeTaken();
    }

    private static class SegmentInfo {
        private final String segmentId;
        private volatile boolean completed = false;
        private final long startTime = System.currentTimeMillis();

        public SegmentInfo(String segmentId) {
            this.segmentId = segmentId;
        }

        public boolean isCompleted() {
            return completed;
        }

        public void setCompleted(boolean completed) {
            this.completed = completed;
        }

        public String getSegmentId() {
            return segmentId;
        }

        public long getStartTime() {
            return startTime;
        }
    }
}
