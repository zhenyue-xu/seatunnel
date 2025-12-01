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

package org.apache.seatunnel.connectors.seatunnel.greenplum.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.greenplum.config.GreenplumConfig;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.greenplum.config.GreenplumConfig.CONNECTOR_IDENTITY;

@AutoService(Factory.class)
public class GreenplumSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        GreenplumConfig.JDBC_URL,
                        GreenplumConfig.DATABASE,
                        GreenplumConfig.TABLE,
                        GreenplumConfig.USERNAME)
                .optional(
                        GreenplumConfig.PASSWORD,
                        GreenplumConfig.BATCH_SIZE,
                        GreenplumConfig.FLUSH_MAX_ROWS,
                        GreenplumConfig.FLUSH_INTERVAL_MS,
                        GreenplumConfig.ENABLE_GPFDIST,
                        GreenplumConfig.GPFDIST_PORT,
                        GreenplumConfig.SAVE_MODE,
                        GreenplumConfig.CONNECTION_TIMEOUT,
                        GreenplumConfig.QUERY_TIMEOUT,
                        GreenplumConfig.ENCODING,
                        GreenplumConfig.DELIMITER,
                        GreenplumConfig.NULL_STRING,
                        GreenplumConfig.USE_BINARY_FORMAT,
                        GreenplumConfig.PRE_WRITE_SQL,
                        GreenplumConfig.POST_WRITE_SQL,
                        GreenplumConfig.DISTRIBUTED_BY,
                        GreenplumConfig.BUFFER_SIZE,
                        GreenplumConfig.USE_TEMP_EXTERNAL_TABLES)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        GreenplumSinkOptions options =
                GreenplumSinkOptions.builder()
                        .url(config.get(GreenplumConfig.JDBC_URL))
                        .database(config.get(GreenplumConfig.DATABASE))
                        .tableName(config.get(GreenplumConfig.TABLE))
                        .username(config.get(GreenplumConfig.USERNAME))
                        .password(config.getOptional(GreenplumConfig.PASSWORD).orElse(null))
                        .batchSize(config.get(GreenplumConfig.BATCH_SIZE))
                        .flushMaxRows(config.get(GreenplumConfig.FLUSH_MAX_ROWS))
                        .flushIntervalMs(config.get(GreenplumConfig.FLUSH_INTERVAL_MS))
                        .enableGpfdist(config.get(GreenplumConfig.ENABLE_GPFDIST))
                        .gpfdistPort(config.get(GreenplumConfig.GPFDIST_PORT))
                        .externalHost(config.getOptional(GreenplumConfig.GPFDIST_HOST).orElse(null))
                        .bindAddress(config.get(GreenplumConfig.GPFDIST_BIND_ADDRESS))
                        .saveMode(config.get(GreenplumConfig.SAVE_MODE))
                        .bufferSize(config.get(GreenplumConfig.BUFFER_SIZE))
                        .connectionTimeout(config.get(GreenplumConfig.CONNECTION_TIMEOUT))
                        .queryTimeout(config.get(GreenplumConfig.QUERY_TIMEOUT))
                        .encoding(config.get(GreenplumConfig.ENCODING))
                        .delimiter(config.get(GreenplumConfig.DELIMITER))
                        .nullString(config.get(GreenplumConfig.NULL_STRING))
                        .useBinaryFormat(config.get(GreenplumConfig.USE_BINARY_FORMAT))
                        .preWriteSql(config.getOptional(GreenplumConfig.PRE_WRITE_SQL).orElse(null))
                        .postWriteSql(
                                config.getOptional(GreenplumConfig.POST_WRITE_SQL).orElse(null))
                        .build();

        // Create sink catalog table
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        CONNECTOR_IDENTITY,
                        config.get(GreenplumConfig.DATABASE),
                        config.get(GreenplumConfig.TABLE));
        CatalogTable sinkCatalogTable = CatalogTable.of(tableIdentifier, catalogTable);

        return () -> new GreenplumSink(options, sinkCatalogTable);
    }
}
