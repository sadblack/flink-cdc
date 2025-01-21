package com.seeyii.flink.cdc.clickhouse.sink;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDriver;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.seeyii.flink.cdc.clickhouse.internal.executor.ClickHouseBatchExecutor;
import com.seeyii.flink.cdc.clickhouse.internal.options.ClickHouseConnectionOptions;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/*
1. write 写入缓存
2. flush 提交至 clickhouse处理
3. close 关闭连接

数据从 flink 映射成 clickhouse
缓存里应该是sql
flush 的时候直接执行


拿到一条数据后，拼接 sql，


在哪儿打开连接？
ClickhouseSinkWriter 提供一个 open 方法，用来创建连接
 */
public class ClickhouseSinkWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseSinkWriter.class);


    private final ClickHouseBatchExecutor<T> executor;

    public ClickhouseSinkWriter(ClickHouseBatchExecutor<T> executor) {
        this.executor = executor;
    }


    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        try {
            executor.addToBatch(element);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {

        try {
            executor.executeBatch();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        //关闭连接
        try {
            executor.closeStatement();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
