package com.seeyii.flink.cdc.clickhouse.sink;

import com.seeyii.flink.cdc.clickhouse.config.ClickHouseOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseCDCSink implements DataSink {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseCDCSink.class);

    private final ClickHouseOptions clickHouseOptions;

    public ClickHouseCDCSink(ClickHouseOptions build) {
        this.clickHouseOptions = build;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        return FlinkSinkProvider.of(new ClickhouseSink(clickHouseOptions));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return schemaChangeEvent -> {};
    }
}
