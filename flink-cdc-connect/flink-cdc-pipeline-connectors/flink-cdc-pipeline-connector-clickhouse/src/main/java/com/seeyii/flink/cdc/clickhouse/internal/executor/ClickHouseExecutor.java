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
import com.seeyii.flink.cdc.clickhouse.internal.converter.ClickHouseTypeConverter;
import com.seeyii.flink.cdc.clickhouse.util.ClickHouseStatementFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import com.seeyii.flink.cdc.clickhouse.internal.connection.ClickHouseConnectionProvider;
import com.seeyii.flink.cdc.clickhouse.internal.connection.ClickHouseStatementWrapper;
import com.seeyii.flink.cdc.clickhouse.internal.options.ClickHouseDmlOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;


/** Executor interface for submitting data to ClickHouse. */
public interface ClickHouseExecutor<T> extends Serializable {

    Logger LOG = LoggerFactory.getLogger(ClickHouseExecutor.class);

    void prepareStatement(ClickHouseConnection connection) throws SQLException;

    void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException;

    void setRuntimeContext(RuntimeContext context);

    void addToBatch(T rowData) throws SQLException;

    void executeBatch() throws SQLException;

    void closeStatement();

    default void attemptExecuteBatch(ClickHousePreparedStatement stmt, int maxRetries)
            throws SQLException {
        for (int i = 0; i <= maxRetries; i++) {
            try {
                stmt.executeBatch();
                return;
            } catch (Exception exception) {
                LOG.error("ClickHouse executeBatch error, retry times = {}", i, exception);
                if (i >= maxRetries) {
                    throw new SQLException(
                            String.format(
                                    "Attempt to execute batch failed, exhausted retry times = %d",
                                    maxRetries),
                            exception);
                }
                try {
                    Thread.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new SQLException(
                            "Unable to flush; interrupted while doing another attempt", ex);
                }
            }
        }
    }

//    static <T> ClickHouseBatchExecutor<T> createBatchExecutor(
//            String tableName,
//            String databaseName,
//            String[] fieldNames,
//            ClickHouseTypeConverter<T> typeConverter,
//            int maxRetries) {
//        String insertSql =
//                ClickHouseStatementFactory.getInsertIntoStatement(
//                        tableName, databaseName, fieldNames);
//        return new ClickHouseBatchExecutor(insertSql, typeConverter, maxRetries);
//    }



}
