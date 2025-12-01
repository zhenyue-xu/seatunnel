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
package org.apache.seatunnel.connectors.seatunnel.greenplum.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.greenplum.config.GreenplumConfig;
import org.apache.seatunnel.connectors.seatunnel.greenplum.source.split.GreenplumSplit;

import com.google.auto.service.AutoService;

import java.util.ArrayList;

@AutoService(Factory.class)
public class GreenplumSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return GreenplumConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        GreenplumConfig.JDBC_URL,
                        GreenplumConfig.USERNAME,
                        GreenplumConfig.PASSWORD)
                .optional(
                        GreenplumConfig.DATABASE,
                        GreenplumConfig.TABLE,
                        GreenplumConfig.QUERY,
                        GreenplumConfig.WHERE_CLAUSE,
                        GreenplumConfig.GPFDIST_PORT,
                        GreenplumConfig.NETWORK_TIMEOUT,
                        GreenplumConfig.BUFFER_SIZE,
                        GreenplumConfig.BATCH_SIZE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource<SeaTunnelRow, GreenplumSplit, ArrayList<GreenplumSplit>>>
            getSourceClass() {
        return GreenplumSource.class;
    }

    @Override
    public TableSource<SeaTunnelRow, GreenplumSplit, ArrayList<GreenplumSplit>> createSource(
            TableSourceFactoryContext context) {
        return () -> {
            ReadonlyConfig config = context.getOptions();
            CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(config);
            return new GreenplumSource(catalogTable, config);
        };
    }
}
