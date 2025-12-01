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
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.greenplum.config.GreenplumConfig;
import org.apache.seatunnel.connectors.seatunnel.greenplum.source.reader.GreenplumReader;
import org.apache.seatunnel.connectors.seatunnel.greenplum.source.split.GreenplumSplit;
import org.apache.seatunnel.connectors.seatunnel.greenplum.source.split.GreenplumSplitEnumerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GreenplumSource
        implements SeaTunnelSource<SeaTunnelRow, GreenplumSplit, ArrayList<GreenplumSplit>> {

    private final CatalogTable catalogTable;
    private final ReadonlyConfig config;

    public GreenplumSource(CatalogTable catalogTable, ReadonlyConfig config) {
        this.catalogTable = catalogTable;
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return GreenplumConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, GreenplumSplit> createReader(
            SourceReader.Context readerContext) {
        return new GreenplumReader(readerContext, config, catalogTable);
    }

    @Override
    public SourceSplitEnumerator<GreenplumSplit, ArrayList<GreenplumSplit>> createEnumerator(
            SourceSplitEnumerator.Context<GreenplumSplit> enumeratorContext) {
        return new GreenplumSplitEnumerator(enumeratorContext, config);
    }

    @Override
    public SourceSplitEnumerator<GreenplumSplit, ArrayList<GreenplumSplit>> restoreEnumerator(
            SourceSplitEnumerator.Context<GreenplumSplit> enumeratorContext,
            ArrayList<GreenplumSplit> checkpointState) {
        return new GreenplumSplitEnumerator(enumeratorContext, config, checkpointState);
    }
}
