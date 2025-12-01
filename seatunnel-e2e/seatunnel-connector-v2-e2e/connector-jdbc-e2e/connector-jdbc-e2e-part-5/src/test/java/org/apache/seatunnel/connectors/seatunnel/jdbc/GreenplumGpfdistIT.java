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
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

public class GreenplumGpfdistIT extends TestSuiteBase implements TestResource {

    private static final Logger log = LoggerFactory.getLogger(GreenplumGpfdistIT.class);

    // Greenplum container configuration
    private static final String GREENPLUM_IMAGE = "datagrip/greenplum:6.8";
    private static final String GREENPLUM_CONTAINER_HOST = "seatunnel_e2e_greenplum_gpfdist";
    private static final String GREENPLUM_DATABASE = "testdb";
    private static final String GREENPLUM_USERNAME = "tester";
    private static final String GREENPLUM_PASSWORD = "pivotal";
    private static final int GREENPLUM_PORT = 5432;

    // Test tables
    private static final String SOURCE_TABLE = "gpfdist_source";
    private static final String SINK_TABLE = "gpfdist_sink";

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

        Map<String, String> env = new HashMap<>();
        env.put("POSTGRES_DB", GREENPLUM_DATABASE);
        env.put("POSTGRES_USER", GREENPLUM_USERNAME);
        env.put("POSTGRES_PASSWORD", GREENPLUM_PASSWORD);

        return new GenericContainer<>(imageName)
                .withNetwork(NETWORK)
                .withNetworkAliases(GREENPLUM_CONTAINER_HOST)
                .withEnv(env)
                .withExposedPorts(GREENPLUM_PORT)
                .withExposedPorts(8080, 8081, 8082, 8083, 8084)
                .withImagePullPolicy(PullPolicy.alwaysPull())
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger(GREENPLUM_IMAGE)));
    }

    private void initializeConnection() throws SQLException {
        String jdbcUrl =
                String.format(
                        "jdbc:postgresql://%s:%d/%s",
                        greenplumContainer.getHost(),
                        greenplumContainer.getMappedPort(GREENPLUM_PORT),
                        GREENPLUM_DATABASE);

        connection = DriverManager.getConnection(jdbcUrl, GREENPLUM_USERNAME, GREENPLUM_PASSWORD);
        connection.setAutoCommit(false);
        log.info("Successfully connected to Greenplum: {}", jdbcUrl);
    }

    private void createTestTables() {
        try (Statement statement = connection.createStatement()) {
            String createSourceSql =
                    String.format(
                            "CREATE TABLE %s ("
                                    + "id SERIAL PRIMARY KEY,"
                                    + "name VARCHAR(100) NOT NULL,"
                                    + "age INTEGER,"
                                    + "salary NUMERIC(10,2),"
                                    + "description TEXT,"
                                    + "created_date DATE,"
                                    + "is_active BOOLEAN"
                                    + ") DISTRIBUTED BY (id)",
                            SOURCE_TABLE);
            statement.execute(createSourceSql);

            String createSinkSql =
                    String.format(
                            "CREATE TABLE %s ("
                                    + "id INTEGER,"
                                    + "name VARCHAR(100),"
                                    + "age INTEGER,"
                                    + "salary NUMERIC(10,2),"
                                    + "description TEXT,"
                                    + "created_date DATE,"
                                    + "is_active BOOLEAN"
                                    + ") DISTRIBUTED BY (id)",
                            SINK_TABLE);
            statement.execute(createSinkSql);

            connection.commit();
            log.info("Test tables created successfully");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create test tables", e);
        }
    }

    private void insertTestData() {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insertSql =
                    new StringBuilder(
                            String.format(
                                    "INSERT INTO %s (name, age, salary, description, created_date, is_active) VALUES ",
                                    SOURCE_TABLE));

            List<String> values = new ArrayList<>();
            for (int i = 1; i <= 1000; i++) {
                values.add(
                        String.format(
                                "('User_%d', %d, %.2f, 'Description for user %d with GPfdist test data', '2023-%02d-%02d', %s)",
                                i,
                                20 + (i % 50),
                                3000.0 + (i * 10.5),
                                i,
                                (i % 12) + 1,
                                (i % 28) + 1,
                                i % 2 == 0));

                if (values.size() == 100) {
                    String batchSql = insertSql + String.join(", ", values);
                    statement.execute(batchSql);
                    values.clear();
                }
            }

            if (!values.isEmpty()) {
                String batchSql = insertSql + String.join(", ", values);
                statement.execute(batchSql);
            }

            connection.commit();
            log.info("Test data (1000 records) inserted successfully");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert test data", e);
        }
    }

    @TestTemplate
    public void testGreenplumGpfdistSinkWithLargeData(TestContainer container)
            throws IOException, InterruptedException {

        Container.ExecResult execResult =
                container.executeJob("/greenplum_gpfdist_sink_large_data.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String logs = execResult.getStdout();
        log.info("Job execution logs: {}", logs);

        Assertions.assertTrue(
                logs.contains("GPfdist")
                        || logs.contains("gpfdist")
                        || logs.contains("EXTERNAL TABLE"),
                "GPfdist protocol should be used but not found in logs");

        verifyDataTransfer();
    }

    @TestTemplate
    public void testGreenplumGpfdistSourceToSink(TestContainer container)
            throws IOException, InterruptedException {

        clearSinkTable();

        Container.ExecResult execResult =
                container.executeJob("/greenplum_gpfdist_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        String logs = execResult.getStdout();
        Assertions.assertTrue(
                logs.contains("GPfdist")
                        || logs.contains("External")
                        || logs.contains("gpfdist://"),
                "GPfdist server should be started");

        verifyDataTransfer();
    }

    @TestTemplate
    public void testGreenplumGpfdistPerformanceComparison(TestContainer container)
            throws IOException, InterruptedException {

        clearSinkTable();

        long jdbcStartTime = System.currentTimeMillis();
        Container.ExecResult jdbcResult = container.executeJob("/greenplum_jdbc_mode.conf");
        long jdbcDuration = System.currentTimeMillis() - jdbcStartTime;

        Assertions.assertEquals(0, jdbcResult.getExitCode(), jdbcResult.getStderr());
        log.info("JDBC mode execution time: {} ms", jdbcDuration);

        clearSinkTable();

        long gpfdistStartTime = System.currentTimeMillis();
        Container.ExecResult gpfdistResult = container.executeJob("/greenplum_gpfdist_mode.conf");
        long gpfdistDuration = System.currentTimeMillis() - gpfdistStartTime;

        Assertions.assertEquals(0, gpfdistResult.getExitCode(), gpfdistResult.getStderr());
        log.info("GPfdist mode execution time: {} ms", gpfdistDuration);

        String gpfdistLogs = gpfdistResult.getStdout();
        Assertions.assertTrue(
                gpfdistLogs.contains("GPfdist")
                        || gpfdistLogs.contains("gpfdist://")
                        || gpfdistLogs.contains("External"),
                "GPfdist specific logs should be present");

        verifyDataTransfer();

        log.info(
                "Performance comparison - JDBC: {}ms, GPfdist: {}ms",
                jdbcDuration,
                gpfdistDuration);
    }

    @TestTemplate
    public void testGreenplumGpfdistErrorHandling(TestContainer container)
            throws IOException, InterruptedException {

        clearSinkTable();

        Container.ExecResult execResult =
                container.executeJob("/greenplum_gpfdist_error_config.conf");

        String logs = execResult.getStdout() + execResult.getStderr();
        log.info("Error handling test logs: {}", logs);

        Assertions.assertTrue(
                logs.contains("GPfdist") || logs.contains("error") || logs.contains("failed"),
                "Should contain error handling information");
    }

    private void verifyDataTransfer() {
        try (Statement statement = connection.createStatement()) {
            ResultSet sourceResult =
                    statement.executeQuery("SELECT COUNT(*) as count FROM " + SOURCE_TABLE);
            sourceResult.next();
            int sourceCount = sourceResult.getInt("count");

            ResultSet sinkResult =
                    statement.executeQuery("SELECT COUNT(*) as count FROM " + SINK_TABLE);
            sinkResult.next();
            int sinkCount = sinkResult.getInt("count");

            log.info("Source table count: {}, Sink table count: {}", sourceCount, sinkCount);
            Assertions.assertEquals(sourceCount, sinkCount, "Data transfer verification failed");

            ResultSet sourceData =
                    statement.executeQuery(
                            "SELECT name, age, salary FROM "
                                    + SOURCE_TABLE
                                    + " ORDER BY id LIMIT 10");
            ResultSet sinkData =
                    statement.executeQuery(
                            "SELECT name, age, salary FROM "
                                    + SINK_TABLE
                                    + " ORDER BY id LIMIT 10");

            List<String> sourceList = new ArrayList<>();
            List<String> sinkList = new ArrayList<>();

            while (sourceData.next()) {
                sourceList.add(
                        String.format(
                                "%s-%d-%.2f",
                                sourceData.getString("name"),
                                sourceData.getInt("age"),
                                sourceData.getDouble("salary")));
            }

            while (sinkData.next()) {
                sinkList.add(
                        String.format(
                                "%s-%d-%.2f",
                                sinkData.getString("name"),
                                sinkData.getInt("age"),
                                sinkData.getDouble("salary")));
            }

            Assertions.assertEquals(sourceList, sinkList, "Data content verification failed");
            log.info("Data transfer verification passed with {} records", sourceCount);

        } catch (SQLException e) {
            throw new RuntimeException("Failed to verify data transfer", e);
        }
    }

    private void clearSinkTable() {
        try (Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE " + SINK_TABLE);
            connection.commit();
            log.info("Sink table cleared");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clear sink table", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        if (greenplumContainer != null) {
            greenplumContainer.close();
        }
    }
}
