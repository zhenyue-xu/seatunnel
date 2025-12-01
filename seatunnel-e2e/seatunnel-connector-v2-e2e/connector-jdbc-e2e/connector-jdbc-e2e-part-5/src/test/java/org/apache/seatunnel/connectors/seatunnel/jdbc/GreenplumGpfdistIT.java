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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

public class GreenplumGpfdistIT extends TestSuiteBase implements TestResource {

    private static final Logger log = LoggerFactory.getLogger(GreenplumGpfdistIT.class);

    // Greenplum container configuration
    private static final String GREENPLUM_IMAGE = "datagrip/greenplum:6.8";
    private static final String GREENPLUM_CONTAINER_HOST = "seatunnel_e2e_greenplum_gpfdist";
    private static final String GREENPLUM_DATABASE = "testdb";
    private static final String GREENPLUM_USERNAME = "gpadmin";
    private static final String GREENPLUM_PASSWORD = "";
    private static final int GREENPLUM_PORT = 5432;

    // Test tables
    private static final String SOURCE_TABLE = "gpfdist_source";
    private static final String SINK_TABLE = "gpfdist_sink";
    private static final String TEST_TABLE = "gpfdist_test";

    private GenericContainer<?> greenplumContainer;
    private Connection connection;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Greenplum-Gpfdist/lib && cd /tmp/seatunnel/plugins/Greenplum-Gpfdist/lib && "
                                        + "wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.4/postgresql-42.5.4.jar --no-check-certificate && "
                                        + "wget https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.14/httpclient-4.5.14.jar --no-check-certificate && "
                                        + "wget https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.16/httpcore-4.4.16.jar --no-check-certificate");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() {
        greenplumContainer = createGreenplumContainer();
        Startables.deepStart(Stream.of(greenplumContainer)).join();

        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(this::initializeConnection);

        createTestTables();
        insertTestData();
    }

    private GenericContainer<?> createGreenplumContainer() {
        DockerImageName imageName = DockerImageName.parse(GREENPLUM_IMAGE);

        return new GenericContainer<>(imageName)
                .withNetwork(NETWORK)
                .withNetworkAliases(GREENPLUM_CONTAINER_HOST)
                .withExposedPorts(GREENPLUM_PORT)
                .waitingFor(
                        Wait.forLogMessage(".*Database successfully started.*", 1)
                                .withStartupTimeout(Duration.ofMinutes(10)))
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger(GREENPLUM_IMAGE)));
    }

    private void initializeConnection() throws SQLException {
        String jdbcUrl =
                String.format(
                        "jdbc:postgresql://%s:%d/postgres",
                        greenplumContainer.getHost(),
                        greenplumContainer.getMappedPort(GREENPLUM_PORT));

        // 先连接到默认数据库，关闭自动提交以避免事务问题
        connection = DriverManager.getConnection(jdbcUrl, GREENPLUM_USERNAME, GREENPLUM_PASSWORD);
        connection.setAutoCommit(true); // 关键：设置为自动提交模式

        createDatabaseIfNotExists();

        // 重新连接到测试数据库
        connection.close();
        jdbcUrl =
                String.format(
                        "jdbc:postgresql://%s:%d/%s",
                        greenplumContainer.getHost(),
                        greenplumContainer.getMappedPort(GREENPLUM_PORT),
                        GREENPLUM_DATABASE);
        connection = DriverManager.getConnection(jdbcUrl, GREENPLUM_USERNAME, GREENPLUM_PASSWORD);
        connection.setAutoCommit(false); // 在测试数据库中可以使用事务

        log.info("Successfully connected to Greenplum: {}", jdbcUrl);
    }

    private void createDatabaseIfNotExists() {
        try (Statement statement = connection.createStatement()) {
            // 检查数据库是否存在
            ResultSet rs =
                    statement.executeQuery(
                            "SELECT 1 FROM pg_database WHERE datname = '"
                                    + GREENPLUM_DATABASE
                                    + "'");

            if (!rs.next()) {
                // 数据库不存在，创建它（注意：不能在事务中执行）
                statement.execute("CREATE DATABASE " + GREENPLUM_DATABASE);
                log.info("Created database: {}", GREENPLUM_DATABASE);
            } else {
                log.info("Database {} already exists", GREENPLUM_DATABASE);
            }

        } catch (SQLException e) {
            if (e.getMessage().contains("already exists")) {
                log.info("Database {} already exists", GREENPLUM_DATABASE);
            } else {
                log.warn("Failed to create database: {}", e.getMessage());
            }
        }
    }

    private void createTestTables() {
        try (Statement statement = connection.createStatement()) {
            // 创建源表
            String createSourceSql =
                    String.format(
                            "CREATE TABLE IF NOT EXISTS %s ("
                                    + "id SERIAL PRIMARY KEY,"
                                    + "name VARCHAR(100) NOT NULL,"
                                    + "age INTEGER,"
                                    + "salary NUMERIC(10,2),"
                                    + "description TEXT,"
                                    + "is_active BOOLEAN"
                                    + ") DISTRIBUTED BY (id)",
                            SOURCE_TABLE);
            statement.execute(createSourceSql);

            // 创建目标表
            String createSinkSql =
                    String.format(
                            "CREATE TABLE IF NOT EXISTS %s ("
                                    + "id INTEGER,"
                                    + "name VARCHAR(100),"
                                    + "age INTEGER,"
                                    + "salary NUMERIC(10,2),"
                                    + "description TEXT,"
                                    + "is_active BOOLEAN"
                                    + ") DISTRIBUTED BY (id)",
                            SINK_TABLE);
            statement.execute(createSinkSql);

            // 创建测试表
            String createTestSql =
                    String.format(
                            "CREATE TABLE IF NOT EXISTS %s ("
                                    + "id INTEGER,"
                                    + "name VARCHAR(100),"
                                    + "age INTEGER,"
                                    + "salary NUMERIC(10,2),"
                                    + "description TEXT,"
                                    + "is_active BOOLEAN"
                                    + ") DISTRIBUTED BY (id)",
                            TEST_TABLE);
            statement.execute(createTestSql);

            connection.commit();
            log.info("Test tables created successfully");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                log.warn("Failed to rollback transaction: {}", rollbackEx.getMessage());
            }
            throw new RuntimeException("Failed to create test tables", e);
        }
    }

    private void insertTestData() {
        try (Statement statement = connection.createStatement()) {
            // 清空现有数据
            statement.execute("TRUNCATE TABLE " + SOURCE_TABLE);

            // 批量插入测试数据
            StringBuilder batchInsert = new StringBuilder();
            batchInsert
                    .append("INSERT INTO ")
                    .append(SOURCE_TABLE)
                    .append(" (name, age, salary, description, is_active) VALUES ");

            List<String> values = new ArrayList<>();
            for (int i = 1; i <= 1000; i++) {
                values.add(
                        String.format(
                                "('User_%d', %d, %.2f, 'Test user %d', %s)",
                                i, 20 + (i % 50), 3000.0 + (i * 10.5), i, i % 2 == 0));

                // 每100条记录执行一次批量插入
                if (values.size() == 100) {
                    String sql = batchInsert + String.join(", ", values);
                    statement.execute(sql);
                    values.clear();
                }
            }

            // 插入剩余的记录
            if (!values.isEmpty()) {
                String sql = batchInsert + String.join(", ", values);
                statement.execute(sql);
            }

            connection.commit();
            log.info("Test data (1000 records) inserted successfully");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                log.warn("Failed to rollback transaction: {}", rollbackEx.getMessage());
            }
            throw new RuntimeException("Failed to insert test data", e);
        }
    }

    /** 测试连接器基本连通性 使用 FakeSource -> Greenplum Sink (JDBC模式) 验证基本功能 */
    @TestTemplate
    public void testGreenplumConnectionAndBasicFunctionality(TestContainer container)
            throws IOException, InterruptedException {

        // 清空测试表
        clearTable(TEST_TABLE);

        // 执行连通性测试
        Container.ExecResult execResult = container.executeJob("/greenplum_connection_test.conf");

        Assertions.assertEquals(
                0,
                execResult.getExitCode(),
                "Connection test should succeed. Error: " + execResult.getStderr());

        String logs = execResult.getStdout();
        log.info("Connection test logs: {}", logs);

        // 验证数据是否正确写入
        verifyDataCount(TEST_TABLE, 100); // FakeSource 生成 100 条记录

        log.info("✅ Greenplum connection and basic functionality test passed");
    }

    /** 测试 GPfdist 主流程 Source (Greenplum with GPfdist) -> Sink (Greenplum with GPfdist) */
    @TestTemplate
    public void testGreenplumGpfdistMainFlow(TestContainer container)
            throws IOException, InterruptedException {

        // 清空目标表
        clearTable(SINK_TABLE);

        // 执行主流程测试
        Container.ExecResult execResult = container.executeJob("/greenplum_gpfdist_main_flow.conf");

        Assertions.assertEquals(
                0,
                execResult.getExitCode(),
                "GPfdist main flow test should succeed. Error: " + execResult.getStderr());

        String logs = execResult.getStdout();
        log.info("GPfdist main flow logs: {}", logs);

        // 验证 GPfdist 协议被使用
        Assertions.assertTrue(
                logs.contains("GPfdist")
                        || logs.contains("gpfdist")
                        || logs.contains("External")
                        || logs.contains("EXTERNAL TABLE"),
                "GPfdist protocol should be used in the main flow");

        // 验证数据传输完整性
        verifyDataTransfer();

        log.info("✅ Greenplum GPfdist main flow test passed");
    }

    private void verifyDataTransfer() {
        try (Statement statement = connection.createStatement()) {
            // 检查源表和目标表的记录数
            ResultSet sourceResult =
                    statement.executeQuery("SELECT COUNT(*) as count FROM " + SOURCE_TABLE);
            sourceResult.next();
            int sourceCount = sourceResult.getInt("count");

            ResultSet sinkResult =
                    statement.executeQuery("SELECT COUNT(*) as count FROM " + SINK_TABLE);
            sinkResult.next();
            int sinkCount = sinkResult.getInt("count");

            log.info("Source table count: {}, Sink table count: {}", sourceCount, sinkCount);
            Assertions.assertEquals(sourceCount, sinkCount, "Data transfer count should match");

            // 验证数据内容抽样
            if (sourceCount > 0) {
                ResultSet sourceData =
                        statement.executeQuery(
                                "SELECT name, age, salary FROM "
                                        + SOURCE_TABLE
                                        + " ORDER BY id LIMIT 5");
                ResultSet sinkData =
                        statement.executeQuery(
                                "SELECT name, age, salary FROM "
                                        + SINK_TABLE
                                        + " ORDER BY id LIMIT 5");

                List<String> sourceRows = new ArrayList<>();
                List<String> sinkRows = new ArrayList<>();

                while (sourceData.next()) {
                    sourceRows.add(
                            String.format(
                                    "%s-%d-%.2f",
                                    sourceData.getString("name"),
                                    sourceData.getInt("age"),
                                    sourceData.getDouble("salary")));
                }

                while (sinkData.next()) {
                    sinkRows.add(
                            String.format(
                                    "%s-%d-%.2f",
                                    sinkData.getString("name"),
                                    sinkData.getInt("age"),
                                    sinkData.getDouble("salary")));
                }

                Assertions.assertEquals(sourceRows, sinkRows, "Sample data content should match");
            }

            log.info("Data transfer verification passed with {} records", sourceCount);

        } catch (SQLException e) {
            throw new RuntimeException("Failed to verify data transfer", e);
        }
    }

    private void verifyDataCount(String tableName, int expectedCount) {
        try (Statement statement = connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT COUNT(*) as count FROM " + tableName);
            result.next();
            int actualCount = result.getInt("count");

            log.info("Table {} count: {} (expected: {})", tableName, actualCount, expectedCount);
            Assertions.assertEquals(
                    expectedCount,
                    actualCount,
                    "Data count should match expected value for table " + tableName);

        } catch (SQLException e) {
            throw new RuntimeException("Failed to verify data count for table " + tableName, e);
        }
    }

    private void clearTable(String tableName) {
        try (Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE " + tableName);
            connection.commit();
            log.info("Table {} cleared", tableName);
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                log.warn("Failed to rollback transaction: {}", rollbackEx.getMessage());
            }
            throw new RuntimeException("Failed to clear table " + tableName, e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        if (greenplumContainer != null) {
            greenplumContainer.close();
        }
    }
}
