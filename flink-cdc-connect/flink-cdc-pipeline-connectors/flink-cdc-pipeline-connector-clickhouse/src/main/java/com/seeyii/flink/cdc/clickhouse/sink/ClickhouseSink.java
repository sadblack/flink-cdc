package com.seeyii.flink.cdc.clickhouse.sink;

import com.seeyii.flink.cdc.clickhouse.config.ClickHouseOptions;
import com.seeyii.flink.cdc.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.seeyii.flink.cdc.clickhouse.internal.converter.ClickHouseJDBCData;
import com.seeyii.flink.cdc.clickhouse.internal.converter.ClickHouseTypeConverter;
import com.seeyii.flink.cdc.clickhouse.internal.executor.ClickHouseBatchExecutor;
import com.seeyii.flink.cdc.clickhouse.util.ClickHouseStatementFactory;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ClickhouseSink implements Sink<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseSink.class);


    private ClickHouseConnectionProvider connectionProvider;

    //TODO 处理类型
    private final List<ClickHouseJDBCData.Type> defaultMetaData = Arrays.asList(
            ClickHouseJDBCData.Type.Int64
            , ClickHouseJDBCData.Type.Int64
            , ClickHouseJDBCData.Type.Int32
            , ClickHouseJDBCData.Type.Int64
            , ClickHouseJDBCData.Type.Int64
            , ClickHouseJDBCData.Type.Int64
            , ClickHouseJDBCData.Type.Int64
            , ClickHouseJDBCData.Type.Int64
            , ClickHouseJDBCData.Type.Int64
            , ClickHouseJDBCData.Type.DateTime
            , ClickHouseJDBCData.Type.DateTime
    );
    ClickHouseBatchExecutor<Event> clickHouseBatchExecutor;


    public ClickhouseSink(ClickHouseOptions options) {

        String databaseName = options.getDatabase();
        String tableName = "zone2";
        String[] fieldNames = new String[]{"id", "compCode", "provinceCode", "cityCode", "districtCode", "townCode", "villageCode", "areaCode", "dataStatus", "createTime", "modifyTime"};

        String insertSql =
                ClickHouseStatementFactory.getInsertIntoStatement(
                        tableName, databaseName, fieldNames);

        connectionProvider = new ClickHouseConnectionProvider(options.toConnectionOptions());

        ClickHouseTypeConverter<Event> typeConverter = (event) -> {

            LOG.info("收到 event: {}", event.toString());
            if (event instanceof DataChangeEvent) {
                return applyDataChangeEvent((DataChangeEvent) event);

            }
            return new LinkedList<>();

        };


        clickHouseBatchExecutor = new ClickHouseBatchExecutor<>(
                insertSql
                , typeConverter
                , 2
        );

        try {
            clickHouseBatchExecutor.prepareStatement(connectionProvider.getOrCreateConnection());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<ClickHouseJDBCData> applyDataChangeEvent(DataChangeEvent event) {

        TableId tableId = event.tableId();

        OperationType op = event.op();

        List<ClickHouseJDBCData> result = new LinkedList<>();
        switch (op) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                RecordData after = event.after();
                int size = defaultMetaData.size();
                for (int i = 0; i < size; i++) {
                    ClickHouseJDBCData.Type type = defaultMetaData.get(i);
                    switch (type) {
                        case Int32:
                            result.add(new ClickHouseJDBCData(after.getInt(i), type));
                            break;
                        case Int64:
                            result.add(new ClickHouseJDBCData(after.getLong(i), type));
                            break;
                        case DateTime:
                            result.add(new ClickHouseJDBCData(after.getTimestamp(i, 100), type));
                            break;
                    }
                }
                break;
            case DELETE:
                break;
            default:
                throw new UnsupportedOperationException("Unsupport Operation " + op);
        }
        return result;
    }

    @Override
    public ClickhouseSinkWriter<Event> createWriter(InitContext context) throws IOException {


        return new ClickhouseSinkWriter<>(clickHouseBatchExecutor);
    }
}
