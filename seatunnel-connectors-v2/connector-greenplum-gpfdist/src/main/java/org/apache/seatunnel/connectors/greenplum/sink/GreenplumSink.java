package org.apache.seatunnel.connectors.greenplum.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.greenplum.config.GreenplumConfig;
import org.apache.seatunnel.connectors.greenplum.config.GreenplumOptions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

@AutoService(Factory.class)
public class GreenplumSink implements SeaTunnelSink<SeaTunnelRow, GreenplumSinkState, GreenplumCommitInfo, GreenplumAggregatedCommitInfo>,
                                    SupportMultiTableSink,
                                    Serializable {
    
    private final GreenplumConfig config;
    private CatalogTable catalogTable;
    private JobContext jobContext;
    
    public GreenplumSink(GreenplumConfig config) {
        this.config = config;
    }
    
    public GreenplumSink(ReadonlyConfig pluginConfig) {
        this.config = GreenplumConfig.buildFromConfig(pluginConfig);
    }
    
    @Override
    public String getPluginName() {
        return "GreenplumGpfdist";
    }
    
    @Override
    public void prepare(ReadonlyConfig pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig,
            GreenplumOptions.URL.key(),
            GreenplumOptions.USERNAME.key(),
            GreenplumOptions.PASSWORD.key(),
            GreenplumOptions.TABLE.key());
        
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SINK, result.getMsg());
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
    public SinkWriter<SeaTunnelRow, GreenplumCommitInfo, GreenplumSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new GreenplumSinkWriter(config, catalogTable, context);
    }
    
    @Override
    public Optional<Serializer<GreenplumCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }
    
    @Override
    public Optional<SinkAggregatedCommitter<GreenplumCommitInfo, GreenplumAggregatedCommitInfo>> 
            createAggregatedCommitter() throws IOException {
        return Optional.of(new GreenplumSinkCommitter(config));
    }
    
    @Override
    public Optional<Serializer<GreenplumAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }
    
    @Override
    public Optional<Serializer<GreenplumSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
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
                GreenplumOptions.BATCH_SIZE,
                GreenplumOptions.DELIMITER,
                GreenplumOptions.NULL_STRING,
                GreenplumOptions.QUEUE_SIZE,
                GreenplumOptions.PARALLELISM,
                GreenplumOptions.COLUMN_DEFINITIONS
            )
            .build();
    }
}