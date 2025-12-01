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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.greenplum.config.GreenplumConfig;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.greenplum.config.GreenplumConfig.CONNECTOR_IDENTITY;

@AutoService(Factory.class)
public class GreenplumCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new GreenplumCatalog(
                catalogName,
                options.get(GreenplumConfig.JDBC_URL),
                options.get(GreenplumConfig.USERNAME),
                options.get(GreenplumConfig.PASSWORD),
                options.get(GreenplumConfig.DATABASE));
    }

    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        GreenplumConfig.JDBC_URL,
                        GreenplumConfig.USERNAME,
                        GreenplumConfig.DATABASE)
                .optional(GreenplumConfig.PASSWORD)
                .build();
    }
}
