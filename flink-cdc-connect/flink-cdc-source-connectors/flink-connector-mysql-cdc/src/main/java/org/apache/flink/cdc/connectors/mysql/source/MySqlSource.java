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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.mysql.CustomProxy;
import org.apache.flink.cdc.connectors.mysql.MySqlSourceReaderProxy;
import org.apache.flink.cdc.connectors.mysql.MySqlValidator;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsStateSerializer;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReaderContext;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * The MySQL CDC Source based on FLIP-27 and Watermark Signal Algorithm which supports parallel
 * reading snapshot of table and then continue to capture data change from binlog.
 *
 * <pre>
 *     1. The source supports parallel capturing table change.
 *     2. The source supports checkpoint in split level when read snapshot data.
 *     3. The source doesn't need apply any lock of MySQL.
 * </pre>
 *
 * <pre>{@code
 * MySqlSource
 *     .<String>builder()
 *     .hostname("localhost")
 *     .port(3306)
 *     .databaseList("mydb")
 *     .tableList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .serverId(5400)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>See {@link MySqlSourceBuilder} for more details.
 *
 * @param <T> the output type of the source.
 */
@Internal
public class MySqlSource<T>
        implements Source<T, MySqlSplit, PendingSplitsState>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private static final String ENUMERATOR_SERVER_NAME = "mysql_source_split_enumerator";

    private MySqlSourceConfigFactory configFactory;
    private DebeziumDeserializationSchema<T> deserializationSchema;
    private RecordEmitterSupplier<T> recordEmitterSupplier;

    public MySqlSource() {
    }

    public void setConfigFactory(MySqlSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
    }

    public void setDeserializationSchema(DebeziumDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    public void setRecordEmitterSupplier(RecordEmitterSupplier<T> recordEmitterSupplier) {
        this.recordEmitterSupplier = recordEmitterSupplier;
    }

    // Actions to perform during the snapshot phase.
    // This field is introduced for testing purpose, for example testing if changes made in the
    // snapshot phase are correctly backfilled into the snapshot by registering a pre high watermark
    // hook for generating changes.
    private SnapshotPhaseHooks snapshotHooks = SnapshotPhaseHooks.empty();

    /**
     * Get a MySqlParallelSourceBuilder to build a {@link MySqlSource}.
     *
     * @return a MySql parallel source builder.
     */
    @PublicEvolving
    public static <T> MySqlSourceBuilder<T> builder() {
        return new MySqlSourceBuilder<>();
    }

    MySqlSource(
            MySqlSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        this(
                configFactory,
                deserializationSchema,
                (sourceReaderMetrics, sourceConfig) ->
                        new MySqlRecordEmitter<>(
                                deserializationSchema,
                                sourceReaderMetrics,
                                sourceConfig.isIncludeSchemaChanges()));
    }

    MySqlSource(
            MySqlSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            RecordEmitterSupplier<T> recordEmitterSupplier) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
        this.recordEmitterSupplier = recordEmitterSupplier;
    }

    public MySqlSourceConfigFactory getConfigFactory() {
        return configFactory;
    }

    @Override
    public Boundedness getBoundedness() {
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            return Boundedness.BOUNDED;
        } else {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }
    }

    /*
    从 SplitEnumerator 拉取 split
    1.什么时候拉取
    2.拉取后根据不同类型，分别是怎么处理的
     */
    @Override
    public SourceReader<T, MySqlSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        // 根据当前子任务的索引创建唯一的source config（例如，唯一的服务器ID）
        MySqlSourceConfig sourceConfig =
                configFactory.createConfig(readerContext.getIndexOfSubtask());

        // 创建一个队列，用于存储将要处理的记录元素
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        // 通过反射获取MetricGroup，因为SourceReaderContext不直接提供此方法
        final Method metricGroupMethod = readerContext.getClass().getMethod("metricGroup");
        metricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup = (MetricGroup) metricGroupMethod.invoke(readerContext);

        // 创建并注册Metrics，用于监控SourceReader的性能
        final MySqlSourceReaderMetrics sourceReaderMetrics =
                new MySqlSourceReaderMetrics(metricGroup);
        sourceReaderMetrics.registerMetrics();

        // 包装SourceReaderContext，为其添加特定于MySql的功能
        MySqlSourceReaderContext mySqlSourceReaderContext =
                new MySqlSourceReaderContext(readerContext);

        // 创建一个供应者，它会在需要时提供新的MySqlSplitReader实例
        Supplier<MySqlSplitReader> splitReaderSupplier =
                () ->
                        new MySqlSplitReader(
                                sourceConfig,
                                readerContext.getIndexOfSubtask(),
                                mySqlSourceReaderContext,
                                snapshotHooks);
        RecordEmitter<SourceRecords, T, MySqlSplitState> emitter = recordEmitterSupplier.get(sourceReaderMetrics, sourceConfig);
        Configuration configuration = readerContext.getConfiguration();
        MySqlSourceReader<T> sourceReader = new MySqlSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                emitter,
                configuration,
                mySqlSourceReaderContext,
                sourceConfig);
        // 返回一个新的MySqlSourceReader实例，它负责协调读取和处理数据
        return new MySqlSourceReaderProxy<>(sourceReader).getProxy(
                elementsQueue,
                splitReaderSupplier,
                emitter,
                configuration,
                mySqlSourceReaderContext,
                sourceConfig
        );
    }


    /*
    连接数据源，产生 split，提供给 reader
    1.怎么产生 split
    2.分配给 reader 的策略
     */
    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext) {
        MySqlSourceConfig sourceConfig = configFactory.createConfig(0, ENUMERATOR_SERVER_NAME);

        final MySqlValidator validator = new MySqlValidator(sourceConfig);
        validator.validate();

        final MySqlSplitAssigner splitAssigner;
        if (!sourceConfig.getStartupOptions().isStreamOnly()) {
            try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
                boolean isTableIdCaseSensitive = DebeziumUtils.isTableIdCaseSensitive(jdbc);
                splitAssigner =
                        new MySqlHybridSplitAssigner(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                new ArrayList<>(),
                                isTableIdCaseSensitive);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner = new MySqlBinlogSplitAssigner(sourceConfig);
        }
        Boundedness boundedness = getBoundedness();
        MySqlSourceEnumerator enumerator = new MySqlSourceEnumerator(
                enumContext, sourceConfig, splitAssigner, boundedness);

        return new CustomProxy<>(enumerator).getProxy(en -> {
            en.setContext(enumContext);
            en.setSourceConfig(sourceConfig);
            en.setSplitAssigner(splitAssigner);
            en.setBoundedness(boundedness);
            en.setReadersAwaitingSplit(new TreeSet<>());
            return en;
        });
    }

    @Override
    public SplitEnumerator<MySqlSplit, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<MySqlSplit> enumContext, PendingSplitsState checkpoint) {

        MySqlSourceConfig sourceConfig = configFactory.createConfig(0, ENUMERATOR_SERVER_NAME);

        final MySqlSplitAssigner splitAssigner;
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new MySqlHybridSplitAssigner(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState) checkpoint);
        } else if (checkpoint instanceof BinlogPendingSplitsState) {
            splitAssigner =
                    new MySqlBinlogSplitAssigner(
                            sourceConfig, (BinlogPendingSplitsState) checkpoint);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }
        return new MySqlSourceEnumerator(
                enumContext, sourceConfig, splitAssigner, getBoundedness());
    }

    @Override
    public SimpleVersionedSerializer<MySqlSplit> getSplitSerializer() {
        return MySqlSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsStateSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @VisibleForTesting
    public void setSnapshotHooks(SnapshotPhaseHooks snapshotHooks) {
        this.snapshotHooks = snapshotHooks;
    }

    /** Create a {@link RecordEmitter} for {@link MySqlSourceReader}. */
    @Internal
    @FunctionalInterface
    interface RecordEmitterSupplier<T> extends Serializable {

        RecordEmitter<SourceRecords, T, MySqlSplitState> get(
                MySqlSourceReaderMetrics metrics, MySqlSourceConfig sourceConfig);
    }
}
