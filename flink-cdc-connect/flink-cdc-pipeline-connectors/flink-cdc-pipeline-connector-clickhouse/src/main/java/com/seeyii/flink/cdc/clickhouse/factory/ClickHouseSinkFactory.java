package com.seeyii.flink.cdc.clickhouse.factory;

import com.seeyii.flink.cdc.clickhouse.config.ClickHouseOptions;
import com.seeyii.flink.cdc.clickhouse.sink.ClickHouseCDCSink;
import com.seeyii.flink.cdc.clickhouse.sink.ClickhouseSink;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.seeyii.flink.cdc.clickhouse.config.ClickHouseCDCOptions.*;

public class ClickHouseSinkFactory implements DataSinkFactory {


    //只要必须的
    private static final String TABLE_CREATE_PROPERTIES_PREFIX = "table.create.properties.";
    private static final String STREAM_LOAD_PROP_PREFIX = "sink.properties.";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(TABLE_CREATE_PROPERTIES_PREFIX, STREAM_LOAD_PROP_PREFIX);

        Configuration config = context.getFactoryConfiguration();
        ClickHouseOptions.Builder optionsBuilder = ClickHouseOptions.builder();
        config.getOptional(url).ifPresent(optionsBuilder::setUrl);
        config.getOptional(username).ifPresent(optionsBuilder::setUsername);
        config.getOptional(password).ifPresent(optionsBuilder::setPassword);
        config.getOptional(database).ifPresent(optionsBuilder::setDatabase);
        config.getOptional(tableName).ifPresent(optionsBuilder::setTableName);

        return new ClickHouseCDCSink(optionsBuilder.build());

    }

    @Override
    public String identifier() {
        return "clickhouse";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(url);
        options.add(username);
        options.add(password);
        options.add(database);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(tableName);

        return options;
    }
}
