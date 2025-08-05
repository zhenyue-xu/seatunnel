package org.apache.seatunnel.connectors.greenplum.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.greenplum.config.GreenplumConfig;
import org.apache.seatunnel.connectors.greenplum.config.GreenplumOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import java.io.Serializable;
import java.util.Collections;

@AutoService(Factory.class)
public class GreenplumSource implements SeaTunnelSource<SeaTunnelRow, GreenplumSourceSplit, GreenplumSourceState>,
    SupportParallelism,
    SupportColumnProjection,
    Serializable {

    private final GreenplumConfig config;
    private CatalogTable catalogTable;
    private JobContext jobContext;

    public GreenplumSource(GreenplumConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "GreenplumGpfdist";
    }

    @Override
    public void prepare(org.apache.seatunnel.api.configuration.ReadonlyConfig pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig,
            GreenplumOptions.URL.key(),
            GreenplumOptions.USERNAME.key(),
            GreenplumOptions.PASSWORD.key(),
            GreenplumOptions.TABLE.key());

        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, GreenplumSourceSplit> createReader(SourceReader.Context readerContext)
        throws Exception {
        return new GreenplumSourceReader(config, catalogTable, readerContext);
    }

    @Override
    public Serializer<GreenplumSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<GreenplumSourceSplit, GreenplumSourceState> createEnumerator(
        SourceSplitEnumerator.Context<GreenplumSourceSplit> enumeratorContext) throws Exception {
        return new GreenplumSourceSplitEnumerator(config, catalogTable, enumeratorContext, null);
    }

    @Override
    public SourceSplitEnumerator<GreenplumSourceSplit, GreenplumSourceState> restoreEnumerator(
        SourceSplitEnumerator.Context<GreenplumSourceSplit> enumeratorContext,
        GreenplumSourceState checkpointState) throws Exception {
        return new GreenplumSourceSplitEnumerator(config, catalogTable, enumeratorContext, checkpointState);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
            .required(
                GreenplumOptions.URL,
                GreenplumOptions.USERNAME,
                GreenplumOptions.PASSWORD,
                GreenplumOptions.TABLE
            )
            .optional(
                GreenplumOptions.QUERY,
                GreenplumOptions.PARTITION_COLUMN,
                GreenplumOptions.PARTITION_UPPER_BOUND,
                GreenplumOptions.PARTITION_LOWER_BOUND,
                GreenplumOptions.PARALLELISM
            )
            .build();
    }
}