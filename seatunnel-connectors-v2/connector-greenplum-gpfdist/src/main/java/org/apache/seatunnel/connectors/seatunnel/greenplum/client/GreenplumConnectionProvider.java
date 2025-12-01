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

package org.apache.seatunnel.connectors.seatunnel.greenplum.client;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Slf4j
public class GreenplumConnectionProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final Properties connectionProperties;

    public GreenplumConnectionProvider(
            String jdbcUrl, String username, String password, Properties properties) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.connectionProperties = properties != null ? properties : new Properties();
    }

    public Connection getConnection() throws SQLException {
        Properties props = new Properties();
        props.putAll(connectionProperties);

        if (username != null) {
            props.setProperty("user", username);
        }
        if (password != null) {
            props.setProperty("password", password);
        }

        // Set connection properties for Greenplum
        props.setProperty("preferQueryMode", "simple");
        props.setProperty("reWriteBatchedInserts", "true");

        return DriverManager.getConnection(jdbcUrl, props);
    }

    public void testConnection() throws SQLException {
        try (Connection conn = getConnection()) {
            if (!conn.isValid(5)) {
                throw new SQLException("Connection is not valid");
            }
            log.info("Successfully tested Greenplum connection");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String jdbcUrl;
        private String username;
        private String password;
        private Properties properties;

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public GreenplumConnectionProvider build() {
            return new GreenplumConnectionProvider(jdbcUrl, username, password, properties);
        }
    }
}
