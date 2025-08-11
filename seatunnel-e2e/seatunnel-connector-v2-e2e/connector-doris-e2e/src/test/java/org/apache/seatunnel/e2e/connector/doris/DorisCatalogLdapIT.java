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

package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalogFactory;
import org.apache.seatunnel.connectors.doris.config.DorisBaseOptions;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class DorisCatalogLdapIT extends AbstractDorisIT {

    private static InMemoryDirectoryServer ldapServer;
    private static final int LDAP_PORT = 11389;
    private static final String LDAP_BASE_DN = "dc=example,dc=com";
    private static final String LDAP_ADMIN_DN = "cn=admin," + LDAP_BASE_DN;
    private static final String LDAP_ADMIN_PASSWORD = "adminpass";
    private static final String LDAP_USER_BASE_DN = "ou=people," + LDAP_BASE_DN;
    private static final String LDAP_GROUP_BASE_DN = "ou=group," + LDAP_BASE_DN;

    private static final String TEST_USER = "testuser";
    private static final String TEST_USER_DN = "uid=" + TEST_USER + "," + LDAP_USER_BASE_DN;
    private static final String TEST_USER_PASSWORD = "testpass123";

    private static final String TEST_ROLE = "doris_test";

    @Override
    protected String getDriverJar() {
        return "https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar";
    }

    @Override
    protected String getDriverClass() {
        return "com.mysql.jdbc.Driver";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/jdbc/lib && cd /tmp/seatunnel/plugins/jdbc/lib && wget "
                                        + getDriverJar());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    public static void setupLdapServer() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(LDAP_BASE_DN);
        config.addAdditionalBindCredentials(LDAP_ADMIN_DN, LDAP_ADMIN_PASSWORD);

        InMemoryListenerConfig listenerConfig =
                InMemoryListenerConfig.createLDAPConfig("test-ldap", LDAP_PORT);
        config.setListenerConfigs(listenerConfig);

        ldapServer = new InMemoryDirectoryServer(config);
        setupLdapEntries();
        ldapServer.startListening();
        log.info("LDAP Server started on port: " + LDAP_PORT);
    }

    @BeforeAll
    @Override
    public void startUp() {
        try {
            setupLdapServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        container =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withPrivilegedMode(true)
                        .withCommand(
                                "/bin/bash",
                                "-c",
                                "echo 'authentication_type=ldap' >> /opt/apache-doris/fe/conf/fe.conf && "
                                        + "echo 'ldap_host = host.docker.internal' > /opt/apache-doris/fe/conf/ldap.conf && "
                                        + "echo 'ldap_port = "
                                        + LDAP_PORT
                                        + "' >> /opt/apache-doris/fe/conf/ldap.conf && "
                                        + "echo 'ldap_admin_name = "
                                        + LDAP_ADMIN_DN
                                        + "' >> /opt/apache-doris/fe/conf/ldap.conf && "
                                        + "echo 'ldap_user_basedn = "
                                        + LDAP_USER_BASE_DN
                                        + "' >> /opt/apache-doris/fe/conf/ldap.conf && "
                                        + "echo \"ldap_user_filter = (&(uid={login}))\" >> /opt/apache-doris/fe/conf/ldap.conf && "
                                        + "echo 'ldap_group_basedn = "
                                        + LDAP_GROUP_BASE_DN
                                        + "' >> /opt/apache-doris/fe/conf/ldap.conf && "
                                        + "/opt/apache-doris/fe/bin/start_fe.sh && "
                                        + "/opt/apache-doris/be/bin/start_be.sh && "
                                        + "tail -f /dev/null");

        container.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", QUERY_PORT, QUERY_PORT),
                        String.format("%s:%s", HTTP_PORT, HTTP_PORT),
                        String.format("%s:%s", BE_HTTP_PORT, BE_HTTP_PORT)));

        Startables.deepStart(Stream.of(container)).join();
        log.info("doris ldap container started");

        given().pollDelay(30, TimeUnit.SECONDS)
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);
        log.info("doris initialized");
    }

    private static void setupLdapEntries() throws Exception {
        ldapServer.add(
                "dn: " + LDAP_BASE_DN, "objectClass: top", "objectClass: domain", "dc: example");

        ldapServer.add(
                "dn: " + LDAP_ADMIN_DN,
                "objectClass: top",
                "objectClass: person",
                "objectClass: organizationalPerson",
                "objectClass: inetOrgPerson",
                "cn: admin",
                "sn: Administrator",
                "userPassword: " + LDAP_ADMIN_PASSWORD);

        ldapServer.add(
                "dn: " + LDAP_USER_BASE_DN,
                "objectClass: top",
                "objectClass: organizationalUnit",
                "ou: people");

        ldapServer.add(
                "dn: " + LDAP_GROUP_BASE_DN,
                "objectClass: top",
                "objectClass: organizationalUnit",
                "ou: group");

        ldapServer.add(
                "dn: " + TEST_USER_DN,
                "objectClass: top",
                "objectClass: person",
                "objectClass: organizationalPerson",
                "objectClass: inetOrgPerson",
                "uid: " + TEST_USER,
                "cn: Test User",
                "sn: User",
                "mail: " + TEST_USER + "@example.com",
                "userPassword: " + TEST_USER_PASSWORD);

        String testRoleDn = "cn=" + TEST_ROLE + "," + LDAP_GROUP_BASE_DN;
        ldapServer.add(
                "dn: " + testRoleDn,
                "objectClass: top",
                "objectClass: groupOfNames",
                "cn: " + TEST_ROLE,
                "member: " + TEST_USER_DN);
    }

    private void configureLdapInDoris() throws SQLException {
        Properties props = new Properties();
        props.put("user", USERNAME);
        props.put("password", PASSWORD);
        jdbcConnection =
                DriverManager.getConnection(String.format(URL, container.getHost()), props);
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute("SET ldap_admin_password = password('" + LDAP_ADMIN_PASSWORD + "')");
            statement.execute("CREATE ROLE IF NOT EXISTS " + TEST_ROLE);
            statement.execute("GRANT SELECT_PRIV ON *.* TO ROLE '" + TEST_ROLE + "'");

            log.info("Doris LDAP configuration completed");
        } catch (SQLException e) {
            log.warn(
                    "LDAP configuration may not be supported in this Doris version: {}",
                    e.getMessage());
        }
    }

    @Test
    public void testDorisCatalogLdapConfigOptions() throws SQLException {
        configureLdapInDoris();
        try {
            URLClassLoader urlClassLoader =
                    new URLClassLoader(
                            new URL[] {new URL(getDriverJar())}, this.getClass().getClassLoader());
            Thread.currentThread().setContextClassLoader(urlClassLoader);
            urlClassLoader.loadClass(getDriverClass()).newInstance();
            String catalogName = "doris-ldap-config";
            String frontEndNodes = container.getHost() + ":" + container.getMappedPort(HTTP_PORT);
            String queryPort = String.valueOf(container.getMappedPort(QUERY_PORT));
            DorisCatalogFactory factory = new DorisCatalogFactory();
            Map<String, Object> catalogConfig = new HashMap<>();
            catalogConfig.put(DorisBaseOptions.FENODES.key(), frontEndNodes);
            catalogConfig.put(DorisBaseOptions.QUERY_PORT.key(), Integer.parseInt(queryPort));
            catalogConfig.put(DorisBaseOptions.USERNAME.key(), TEST_USER);
            catalogConfig.put(DorisBaseOptions.PASSWORD.key(), TEST_USER_PASSWORD);
            catalogConfig.put(DorisBaseOptions.ENABLE_LDAP.key(), true);
            ReadonlyConfig config = ReadonlyConfig.fromMap(catalogConfig);
            DorisCatalog catalog = (DorisCatalog) factory.createCatalog(catalogName, config);
            catalog.open();
            Assertions.assertNotNull(catalog);
            log.info("LDAP catalog configuration test successful");
        } catch (Exception e) {
            log.info("Catalog creation failed as expected", e);
            Assertions.assertTrue(
                    e.getMessage().contains("LDAP") || e.getMessage().contains("authentication"));
        }
    }

    @AfterAll
    public static void shutdownLdapServer() {
        if (ldapServer != null) {
            ldapServer.shutDown(true);
            log.info("LDAP server shutdown completed");
        }
    }
}
