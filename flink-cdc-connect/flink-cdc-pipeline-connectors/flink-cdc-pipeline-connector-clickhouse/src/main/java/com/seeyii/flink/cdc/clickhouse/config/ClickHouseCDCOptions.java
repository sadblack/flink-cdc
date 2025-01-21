package com.seeyii.flink.cdc.clickhouse.config;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

public class ClickHouseCDCOptions {

    public static final ConfigOption<String> url =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the clickhouse url");

    public static final ConfigOption<String> username =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the clickhouse user name.");
    public static final ConfigOption<String> password =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the clickhouse password.");

    public static final ConfigOption<String> database =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the clickhouse database");

    public static final ConfigOption<String> tableName =
            ConfigOptions.key("tableName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the clickhouse table name");
}
