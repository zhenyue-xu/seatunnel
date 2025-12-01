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

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class GpfdistProtocol {

    private static final String HTTP_OK_RESPONSE =
            "HTTP/1.1 200 OK\r\n" + "Content-Type: text/plain\r\n" + "Connection: close\r\n\r\n";

    private static final String HTTP_NOT_FOUND_RESPONSE =
            "HTTP/1.1 404 Not Found\r\n"
                    + "Content-Type: text/plain\r\n"
                    + "Connection: close\r\n\r\n";

    public static void handleRequest(Socket clientSocket, BlockingQueue<String> dataQueue) {
        try (BufferedReader in =
                        new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                OutputStream out = clientSocket.getOutputStream()) {

            String requestLine = in.readLine();
            if (requestLine == null) {
                return;
            }

            log.debug("Received request: {}", requestLine);

            // Parse HTTP request
            String[] parts = requestLine.split(" ");
            if (parts.length < 2) {
                sendErrorResponse(out);
                return;
            }

            String method = parts[0];
            String path = parts[1];

            // Skip headers
            String line;
            while ((line = in.readLine()) != null && !line.isEmpty()) {
                // Skip HTTP headers
            }

            if ("GET".equals(method) && path.startsWith("/")) {
                handleGetRequest(out, dataQueue);
            } else {
                sendErrorResponse(out);
            }

        } catch (IOException e) {
            log.error("Error handling GPfdist request", e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                log.warn("Error closing client socket", e);
            }
        }
    }

    private static void handleGetRequest(OutputStream out, BlockingQueue<String> dataQueue)
            throws IOException {
        out.write(HTTP_OK_RESPONSE.getBytes());

        // Stream data from queue
        try {
            while (!Thread.currentThread().isInterrupted()) {
                String data = dataQueue.poll();
                if (data == null) {
                    break; // No more data
                }

                out.write(data.getBytes());
                out.write('\n');
            }
        } catch (Exception e) {
            log.debug("Client disconnected or error occurred", e);
        }

        out.flush();
    }

    private static void sendErrorResponse(OutputStream out) throws IOException {
        out.write(HTTP_NOT_FOUND_RESPONSE.getBytes());
        out.flush();
    }
}
