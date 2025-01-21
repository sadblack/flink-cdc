/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.seeyii.flink.cdc.clickhouse.internal.executor;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import com.seeyii.flink.cdc.clickhouse.internal.converter.ClickHouseJDBCData;
import com.seeyii.flink.cdc.clickhouse.internal.converter.ClickHouseTypeConverter;
import org.apache.flink.api.common.functions.RuntimeContext;
import com.seeyii.flink.cdc.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.seeyii.flink.cdc.clickhouse.internal.connection.ClickHouseStatementWrapper;
import org.apache.flink.cdc.common.data.TimestampData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import static com.seeyii.flink.cdc.clickhouse.internal.converter.ClickHouseJDBCData.Type.*;

/** ClickHouse's batch executor. */
public class ClickHouseBatchExecutor<T> implements ClickHouseExecutor<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);

    private final String insertSql;

    private final ClickHouseTypeConverter<T> typeConverter;

    private final int maxRetries;

    private transient ClickHousePreparedStatement statement;

    private transient ClickHouseConnectionProvider connectionProvider;

    /*
    1. insertSql 需要读取数据源，获取所有字段的原信息 （只适合 该表已存在的情况）
    2. convert 提供从 flink type 转到 clickhouse type 的功能
     */
    public ClickHouseBatchExecutor(
            String insertSql, ClickHouseTypeConverter<T> typeConverter, int maxRetries) {
        this.insertSql = insertSql;
        this.typeConverter = typeConverter;
        this.maxRetries = maxRetries;
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        statement = (ClickHousePreparedStatement) connection.prepareStatement(insertSql);
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider)
            throws SQLException {
        this.connectionProvider = connectionProvider;
        prepareStatement(connectionProvider.getOrCreateConnection());
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {}

    @Override
    public void addToBatch(T record) throws SQLException {

        //只处理 添加
        //TODO 处理类型
        List<ClickHouseJDBCData> convertedList = typeConverter.convert(record);

        if (!convertedList.isEmpty()) {
            int index = 1;

            for (ClickHouseJDBCData value : convertedList) {

                switch (value.getType()) {
                    case Int32:
                        statement.setInt(index++, (int) value.getData());
                        break;
                    case Int64:
                        statement.setLong(index++, (long) value.getData());
                        break;
                    case DateTime:
                        statement.setTimestamp(index++, new Timestamp(((TimestampData) value.getData()).getMillisecond()));
                        break;
                    default:
                        throw new RuntimeException("暂不支持此类型");
//                case BLOB -> statement.setBytes(1, (byte[]) value.getData());
//                case BOOLEAN -> statement.setBoolean(1, (boolean) value.getData());
//                case TINYINT -> statement.setByte(1, (byte) value.getData());
//                case SMALLINT -> statement.setShort(1, (short) value.getData());
//                case INTEGER -> statement.setInt(1, (int) value.getData());
//                case BIGINT -> statement.setLong(1, (long) value.getData());
//                case FLOAT -> statement.setFloat(1, (float) value.getData());
//                case DOUBLE -> statement.setDouble(1, (double) value.getData());
//                case STRING, CHAR -> statement.setString(1, (String) value.getData());
//                case DATE -> statement.setDate(1, (java.sql.Date) value.getData());
                }
            }

            statement.addBatch();
        }

//        statement.setBoolean();
//        statement.setByte();
//        statement.setShort();
//        statement.setInt();
//        statement.setFloat();
//        statement.setDouble();
//        statement.setLong();
//        statement.setString();
//        statement.setDate();
//        statement.setTime();
//        statement.setTimestamp();
//
//        statement.setArray();
//        statement.setBytes();
//        statement.setObject();
//
//        statement.setBlob();
//        statement.setNull();
    }

    @Override
    public void executeBatch() throws SQLException {
        attemptExecuteBatch(statement, maxRetries);
    }

    @Override
    public void closeStatement() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException exception) {
                LOG.warn("ClickHouse batch statement could not be closed.", exception);
            } finally {
                statement = null;
            }
        }
    }

    @Override
    public String toString() {
        return "ClickHouseBatchExecutor{"
                + "insertSql='"
                + insertSql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
