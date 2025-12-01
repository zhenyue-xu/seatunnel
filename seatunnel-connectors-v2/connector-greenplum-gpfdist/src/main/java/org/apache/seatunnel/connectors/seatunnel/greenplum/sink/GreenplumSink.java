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

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.greenplum.config.GreenplumConfig;
import org.apache.seatunnel.connectors.seatunnel.greenplum.sink.state.GreenplumCommitInfo;

import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class GreenplumSink implements SeaTunnelSink<SeaTunnelRow, Void, GreenplumCommitInfo, Void> {

    private final GreenplumSinkOptions options;
    private final CatalogTable catalogTable;

    public GreenplumSink(GreenplumSinkOptions options, CatalogTable catalogTable) {
        this.options = options;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return GreenplumConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public SinkWriter<SeaTunnelRow, GreenplumCommitInfo, Void> createWriter(
            SinkWriter.Context context) {
        return new GreenplumWriter(options, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
