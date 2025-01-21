package com.seeyii.flink.cdc.clickhouse.internal.converter;

import javax.annotation.Nonnull;
import java.util.List;

@FunctionalInterface
public interface ClickHouseTypeConverter<T> {

    @Nonnull
    List<ClickHouseJDBCData> convert(T type);
}
