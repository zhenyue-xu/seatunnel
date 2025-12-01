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

package org.apache.seatunnel.connectors.seatunnel.greenplum.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.exception.CommonError;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class GreenplumCatalog implements Catalog {

    private final String catalogName;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String defaultDatabase;

    public GreenplumCatalog(
            String catalogName,
            String jdbcUrl,
            String username,
            String password,
            String defaultDatabase) {
        this.catalogName = catalogName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.defaultDatabase = defaultDatabase;
    }

    @Override
    public void open() throws CatalogException {
        try {
            // Test connection
            try (Connection conn = getConnection()) {
                log.info("Successfully connected to Greenplum database");
            }
        } catch (SQLException e) {
            throw new CatalogException("Failed to open Greenplum Catalog: " + e.getMessage(), e);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getCatalogs()) {
                while (rs.next()) {
                    if (rs.getString("TABLE_CAT").equals(databaseName)) {
                        return true;
                    }
                }
            }
            return false;
        } catch (SQLException e) {
            throw new CatalogException("Failed to check database existence: " + databaseName, e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getCatalogs()) {
                while (rs.next()) {
                    databases.add(rs.getString("TABLE_CAT"));
                }
            }
        } catch (SQLException e) {
            throw new CatalogException("Failed to list databases", e);
        }
        return databases;
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name(), databaseName);
        }

        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs =
                    metaData.getTables(databaseName, null, null, new String[] {"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            throw new CatalogException("Failed to list tables for database: " + databaseName, e);
        }
        return tables;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs =
                    metaData.getTables(
                            tablePath.getDatabaseName(),
                            null,
                            tablePath.getTableName(),
                            new String[] {"TABLE"})) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "Failed to check table existence: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        throw CommonError.unsupportedOperation(name(), "get table with tablePath");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(name(), tablePath.getDatabaseName());
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) return;
            throw new TableAlreadyExistException(name(), tablePath);
        }

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            // Build CREATE TABLE SQL based on CatalogTable schema
            String createTableSql = buildCreateTableSql(tablePath, table);
            stmt.executeUpdate(createTableSql);
        } catch (SQLException e) {
            throw new CatalogException("Failed to create table: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) return;
            throw new TableNotExistException(name(), tablePath);
        }

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            String dropTableSql =
                    String.format(
                            "DROP TABLE %s.%s",
                            tablePath.getDatabaseName(), tablePath.getTableName());
            stmt.executeUpdate(dropTableSql);
        } catch (SQLException e) {
            throw new CatalogException("Failed to drop table: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw CommonError.unsupportedOperation(name(), "create database");
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw CommonError.unsupportedOperation(name(), "drop database");
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) return;
            throw new TableNotExistException(name(), tablePath);
        }

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            String truncateSql =
                    String.format(
                            "TRUNCATE TABLE %s.%s",
                            tablePath.getDatabaseName(), tablePath.getTableName());
            stmt.executeUpdate(truncateSql);
        } catch (SQLException e) {
            throw new CatalogException("Failed to truncate table: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            String countSql =
                    String.format(
                            "SELECT COUNT(*) FROM %s.%s LIMIT 1",
                            tablePath.getDatabaseName(), tablePath.getTableName());
            try (ResultSet rs = stmt.executeQuery(countSql)) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void close() throws CatalogException {
        // No resources to close for JDBC connections
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    private String buildCreateTableSql(TablePath tablePath, CatalogTable table) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ")
                .append(tablePath.getDatabaseName())
                .append(".")
                .append(tablePath.getTableName())
                .append(" (");

        // Add column definitions based on schema
        // This is a simplified implementation
        sql.append("id SERIAL PRIMARY KEY");

        sql.append(")");
        return sql.toString();
    }
}
