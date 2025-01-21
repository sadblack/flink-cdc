package com.seeyii.flink.cdc.clickhouse.internal.converter;

import org.apache.flink.cdc.common.data.TimestampData;

public class ClickHouseJDBCData {

    private final Object data;

    private final Type type;

    public ClickHouseJDBCData(Object data, Type type) {
        //TODO 处理类型
        switch (type) {
            /*
            Int32,
            Int64,
            DateTime,
             */
            case DateTime: assert data instanceof TimestampData;
        }

        this.data = data;
        this.type = type;
    }

    public Object getData() {
        return data;
    }

    public Type getType() {
        return type;
    }

    //TODO 处理类型
    //这里应该是 click house jdbc 支持的所有类型，先只写 特定的
    public enum Type {
        Int32,
        Int64,
        DateTime,
    }
}
