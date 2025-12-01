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
package org.apache.seatunnel.connectors.seatunnel.greenplum.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;

public class GreenplumConfig {

    public static final String CONNECTOR_IDENTITY = "Greenplum-Gpfdist";

    public static final Option<String> JDBC_URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Greenplum JDBC connection URL");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database username");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database password");

    public static final Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("Database name");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("Table name");

    public static final Option<Boolean> ENABLE_GPFDIST =
            Options.key("enable.gpfdist")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable GPfdist for high-performance data transfer");

    public static final Option<Long> NETWORK_TIMEOUT =
            Options.key("network.timeout")
                    .longType()
                    .defaultValue(60000L)
                    .withDescription("Network timeout in milliseconds");

    public static final Option<Integer> BUFFER_SIZE =
            Options.key("buffer.size")
                    .intType()
                    .defaultValue(8192)
                    .withDescription("Buffer size for data transfer");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Batch size for bulk operations");

    public static final Option<Integer> FLUSH_MAX_ROWS =
            Options.key("flush.max.rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum number of rows before flushing");

    public static final Option<Long> FLUSH_INTERVAL_MS =
            Options.key("flush.interval.ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription("Flush interval in milliseconds");

    public static final Option<DataSaveMode> SAVE_MODE =
            Options.key("save.mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription("Data save mode");

    public static final Option<Integer> CONNECTION_TIMEOUT =
            Options.key("connection.timeout")
                    .intType()
                    .defaultValue(30)
                    .withDescription("Connection timeout in seconds");

    public static final Option<Integer> QUERY_TIMEOUT =
            Options.key("query.timeout")
                    .intType()
                    .defaultValue(300)
                    .withDescription("Query timeout in seconds");

    public static final Option<String> ENCODING =
            Options.key("encoding")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription("Character encoding");

    public static final Option<String> DELIMITER =
            Options.key("delimiter")
                    .stringType()
                    .defaultValue("\t")
                    .withDescription("Field delimiter");

    public static final Option<String> NULL_STRING =
            Options.key("null.string")
                    .stringType()
                    .defaultValue("\\N")
                    .withDescription("Null value representation");

    public static final Option<Boolean> USE_BINARY_FORMAT =
            Options.key("use.binary.format")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Use binary format for data transfer");

    public static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SQL query for reading data");

    public static final Option<String> WHERE_CLAUSE =
            Options.key("where")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("WHERE clause for filtering data");

    public static final Option<Integer> FETCH_SIZE =
            Options.key("fetch.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("JDBC fetch size");

    public static final Option<String> SPLIT_COLUMN =
            Options.key("split.column")
                    .stringType()
                    .defaultValue("gp_segment_id")
                    .withDescription("Column used for splitting");

    public static final Option<Integer> MAX_SPLITS =
            Options.key("max.splits")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Maximum number of splits");

    public static final Option<String> WRITE_MODE =
            Options.key("write.mode")
                    .stringType()
                    .defaultValue("insert")
                    .withDescription("Write mode: insert, truncate, or append");

    public static final Option<String> DISTRIBUTED_BY =
            Options.key("distributed.by")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Distribution keys for table creation");

    public static final Option<Boolean> USE_TEMP_EXTERNAL_TABLES =
            Options.key("use.temp.external.tables")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Use temporary external tables");

    public static final Option<String> PRE_WRITE_SQL =
            Options.key("pre.write.sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SQL to execute before writing");

    public static final Option<String> POST_WRITE_SQL =
            Options.key("post.write.sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SQL to execute after writing");

    public static final Option<String> GPFDIST_HOST =
            Options.key("gpfdist.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "GPfdist server external host address, auto-detect if not specified");

    public static final Option<String> GPFDIST_BIND_ADDRESS =
            Options.key("gpfdist.bind.address")
                    .stringType()
                    .defaultValue("0.0.0.0")
                    .withDescription("GPfdist server bind address");

    public static final Option<Integer> GPFDIST_PORT =
            Options.key("gpfdist.port")
                    .intType()
                    .defaultValue(0)
                    .withDescription("GPfdist server port, 0 for random port");
}
