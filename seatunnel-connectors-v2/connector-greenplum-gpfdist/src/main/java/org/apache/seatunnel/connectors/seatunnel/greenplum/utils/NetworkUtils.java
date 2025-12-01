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

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.function.Supplier;

@Slf4j
public class NetworkUtils {

    private static final NetworkUtils INSTANCE = new NetworkUtils();

    public static NetworkUtils getInstance() {
        return INSTANCE;
    }

    public HostInfo getLocalHostNameAndIp() {
        String retName = null;
        String retIp = null;

        try {
            Enumeration<NetworkInterface> networkInterfaces =
                    NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = networkInterfaces.nextElement();
                if (netInterface.isUp() && !netInterface.isLoopback()) {
                    Enumeration<InetAddress> inetAddresses = netInterface.getInetAddresses();
                    while (inetAddresses.hasMoreElements()) {
                        InetAddress inetAddress = inetAddresses.nextElement();
                        if (inetAddress.isSiteLocalAddress() && !inetAddress.isLoopbackAddress()) {
                            String hostName = inetAddress.getHostName();
                            String ipAddress = inetAddress.getHostAddress();

                            if (!hostName.equals(ipAddress)) {
                                if (retName == null) {
                                    retName = hostName;
                                    retIp = ipAddress;
                                }

                                try (DatagramSocket socket = new DatagramSocket()) {
                                    socket.connect(InetAddress.getByName("1.1.1.1"), 80);
                                    if (socket.getLocalAddress()
                                            .getHostAddress()
                                            .equals(ipAddress)) {
                                        return new HostInfo(hostName, ipAddress);
                                    }
                                } catch (Exception e) {
                                    log.debug(
                                            "Failed to test default route for {}: {}",
                                            ipAddress,
                                            e.getMessage());
                                }
                            }
                        }
                    }
                }
            }
        } catch (SocketException e) {
            log.warn("Error getting network interfaces: {}", e.getMessage());
        }

        if (retIp == null || retName == null) {
            try {
                InetAddress localHost = InetAddress.getLocalHost();
                retIp = localHost.getHostAddress();
                retName = localHost.getHostName();
            } catch (Exception e) {
                log.warn("Failed to get localhost info: {}", e.getMessage());
                retIp = "127.0.0.1";
                retName = "localhost";
            }
        }

        return new HostInfo(retName, retIp);
    }

    public String resolveHostToIp(String hostName) {
        if (hostName == null || hostName.trim().isEmpty()) {
            return null;
        }

        try {
            InetAddress[] addresses = InetAddress.getAllByName(hostName);
            for (InetAddress address : addresses) {
                if (!address.isLoopbackAddress()) {
                    String ip = address.getHostAddress();
                    log.debug("Resolved {} to {}", hostName, ip);
                    return ip;
                }
            }

            if ("localhost".equals(hostName) || "127.0.0.1".equals(hostName)) {
                HostInfo hostInfo = getLocalHostNameAndIp();
                log.debug("Resolved {} to local {}", hostName, hostInfo.getIpAddress());
                return hostInfo.getIpAddress();
            }
        } catch (Exception e) {
            log.warn("Failed to resolve hostname {}: {}", hostName, e.getMessage());
        }

        return hostName;
    }

    public boolean waitForCompletion(long timeoutMs, Supplier<Boolean> condition) {
        long startTime = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()
                && (timeoutMs < 0 || (System.currentTimeMillis() - startTime) < timeoutMs)) {

            if (condition.get()) {
                return true;
            }

            try {
                Thread.sleep(Math.min(100, Math.max(1, timeoutMs / 100)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return condition.get();
    }

    public boolean isPortAvailable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean isGpfdistHealthy(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), 5000);
            return true;
        } catch (IOException e) {
            log.debug("GPfdist health check failed for {}:{} - {}", host, port, e.getMessage());
            return false;
        }
    }

    public static class HostInfo {
        private final String hostName;
        private final String ipAddress;

        public HostInfo(String hostName, String ipAddress) {
            this.hostName = hostName;
            this.ipAddress = ipAddress;
        }

        public String getHostName() {
            return hostName;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        @Override
        public String toString() {
            return String.format("HostInfo{hostName='%s', ipAddress='%s'}", hostName, ipAddress);
        }
    }
}
