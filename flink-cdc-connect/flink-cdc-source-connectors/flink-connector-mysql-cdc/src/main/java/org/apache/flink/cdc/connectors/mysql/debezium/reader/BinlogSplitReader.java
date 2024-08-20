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

package org.apache.flink.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupMode;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * A Debezium binlog reader implementation that also support reads binlog and filter overlapping
 * snapshot data that {@link SnapshotSplitReader} read.
 */
public class BinlogSplitReader implements DebeziumReader<SourceRecords, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogSplitReader.class);
    private StatefulTaskContext statefulTaskContext;
    private ExecutorService executorService;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;

    private MySqlBinlogSplitReadTask binlogSplitReadTask;
    private MySqlBinlogSplit currentBinlogSplit;
    private Map<TableId, List<FinishedSnapshotSplitInfo>> finishedSplitsInfo;
    // tableId -> the max splitHighWatermark
    private Map<TableId, BinlogOffset> maxSplitHighWatermarkMap;
    private Set<TableId> pureBinlogPhaseTables;
    private Tables.TableFilter capturedTableFilter;
    private final StoppableChangeEventSourceContext changeEventSourceContext =
            new StoppableChangeEventSourceContext();

    private static final long READER_CLOSE_TIMEOUT = 30L;

    public BinlogSplitReader() {
    }

    public void setStatefulTaskContext(StatefulTaskContext statefulTaskContext) {
        this.statefulTaskContext = statefulTaskContext;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public void setCurrentTaskRunning(boolean currentTaskRunning) {
        this.currentTaskRunning = currentTaskRunning;
    }

    public void setPureBinlogPhaseTables(Set<TableId> pureBinlogPhaseTables) {
        this.pureBinlogPhaseTables = pureBinlogPhaseTables;
    }

    public BinlogSplitReader(StatefulTaskContext statefulTaskContext, int subTaskId) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("binlog-reader-" + subTaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = true;
        this.pureBinlogPhaseTables = new HashSet<>();
    }

    public void submitSplit(MySqlSplit mySqlSplit) {
        LOG.info(String.format("开始执行binlogsplit: %s", mySqlSplit));
        this.currentBinlogSplit = mySqlSplit.asBinlogSplit();
        configureFilter();
        statefulTaskContext.configure(currentBinlogSplit);
        this.capturedTableFilter =
                statefulTaskContext.getConnectorConfig().getTableFilters().dataCollectionFilter();
        this.queue = statefulTaskContext.getQueue();
        this.binlogSplitReadTask =
                new MySqlBinlogSplitReadTask(
                        statefulTaskContext.getConnectorConfig(),
                        statefulTaskContext.getConnection(),
                        statefulTaskContext.getDispatcher(),
                        statefulTaskContext.getSignalEventDispatcher(),
                        statefulTaskContext.getErrorHandler(),
                        StatefulTaskContext.getClock(),
                        statefulTaskContext.getTaskContext(),
                        (MySqlStreamingChangeEventSourceMetrics)
                                statefulTaskContext.getStreamingChangeEventSourceMetrics(),
                        currentBinlogSplit,
                        createEventFilter());

        executorService.submit(
                () -> {
                    try {
                        binlogSplitReadTask.execute(
                                changeEventSourceContext,
                                statefulTaskContext.getMySqlPartition(),
                                statefulTaskContext.getOffsetContext());
                    } catch (Exception e) {
                        LOG.error(
                                String.format(
                                        "Execute binlog read task for mysql split %s fail",
                                        currentBinlogSplit),
                                e);
                        readException = e;
                    } finally {
                        stopBinlogReadTask();
                    }
                });
    }

    @Override
    public boolean isFinished() {
        return currentBinlogSplit == null || !currentTaskRunning;
    }

    @Nullable
    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (currentTaskRunning) {
            List<DataChangeEvent> batch = queue.poll();
            for (DataChangeEvent event : batch) {
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                }
            }
            List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(sourceRecords));
            return sourceRecordsSet.iterator();
        } else {
            return null;
        }
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentBinlogSplit, readException.getMessage()),
                    readException);
        }
    }

    @Override
    public void close() {
        try {
            if (statefulTaskContext.getConnection() != null) {
                statefulTaskContext.getConnection().close();
            }
            if (statefulTaskContext.getBinaryLogClient() != null) {
                statefulTaskContext.getBinaryLogClient().disconnect();
            }

            stopBinlogReadTask();
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the binlog split reader in {} seconds.",
                            READER_CLOSE_TIMEOUT);
                }
            }
            statefulTaskContext.getDatabaseSchema().close();
        } catch (Exception e) {
            LOG.error("Close binlog reader error", e);
        }
    }

    /**
     * 判断记录是否应该被发送。
     *
     * <p>水印信号算法专为 binlog 分割读取器设计，仅发送属于已完成快照分割的 binlog 事件。对于每个快照分割，
     * 只有当 binlog 事件的偏移量在其高水位之后时才认为有效。
     *
     * <pre> 示例: 输入数据为 :
     *    快照分割-0 信息 : [0,    1024) 高水位0
     *    快照分割-1 信息 : [1024, 2048) 高水位1
     *  输出数据为:
     *  只有属于 [0,    1024) 且偏移量在高水位0之后的 binlog 事件应发送，
     *  只有属于 [1024, 2048) 且偏移量在高水位1之后的 binlog 事件应发送。
     * </pre>
     *
     * @param sourceRecord 待判断的源记录
     * @return 如果记录应该被发送则返回 true，否则返回 false
     */
private boolean shouldEmit(SourceRecord sourceRecord) {
    // 检查是否为数据变更记录（例如：INSERT, UPDATE, DELETE）
    if (RecordUtils.isDataChangeRecord(sourceRecord)) {
        TableId tableId = RecordUtils.getTableId(sourceRecord);
        // 如果此表处于纯 binlog 阶段，直接发送
        if (pureBinlogPhaseTables.contains(tableId)) {
            return true;
        }
        BinlogOffset position = RecordUtils.getBinlogPosition(sourceRecord);
        // 检查是否已进入纯 binlog 阶段
        if (hasEnterPureBinlogPhase(tableId, position)) {
            return true;
        }

        // 只有捕获了快照分割的表需要过滤
        if (finishedSplitsInfo.containsKey(tableId)) {
            // 获取表的分割键列类型
            RowType splitKeyType =
                    ChunkUtils.getChunkKeyColumnType(
                            statefulTaskContext.getDatabaseSchema().tableFor(tableId),
                            statefulTaskContext.getSourceConfig().getChunkKeyColumns());

            // 从记录中提取包含分割键的结构
            Struct target = RecordUtils.getStructContainsChunkKey(sourceRecord);
            // 计算分割键
            Object[] chunkKey =
                    RecordUtils.getSplitKey(
                            splitKeyType, statefulTaskContext.getSchemaNameAdjuster(), target);
            // 遍历所有完成的快照分割信息
            for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo.get(tableId)) {
                // 检查分割键是否在分割范围内并且在高水位之后
                if (RecordUtils.splitKeyRangeContains(
                                chunkKey, splitInfo.getSplitStart(), splitInfo.getSplitEnd())
                        && position.isAfter(splitInfo.getHighWatermark())) {
                    return true;
                }
            }
        }
        // 如果不在监控的分割范围内，则不应发送
        return false;
    } else if (RecordUtils.isSchemaChangeEvent(sourceRecord)) {
        // 如果为表结构变更记录
        if (RecordUtils.isTableChangeRecord(sourceRecord)) {
            TableId tableId = RecordUtils.getTableId(sourceRecord);
            // 只有当表在捕获范围内时才发送
            return capturedTableFilter.isIncluded(tableId);
        } else {
            // 与表结构无关的变更，如 `CREATE/DROP DATABASE`，忽略
            return false;
        }
    }
    // 默认发送
    return true;
}


    private boolean hasEnterPureBinlogPhase(TableId tableId, BinlogOffset position) {
        // the existed tables those have finished snapshot reading
        if (maxSplitHighWatermarkMap.containsKey(tableId)
                && position.isAtOrAfter(maxSplitHighWatermarkMap.get(tableId))) {
            pureBinlogPhaseTables.add(tableId);
            return true;
        }

        // Use still need to capture new sharding table if user disable scan new added table,
        // The history records for all new added tables(including sharding table and normal table)
        // will be capture after restore from a savepoint if user enable scan new added table
        if (!statefulTaskContext.getSourceConfig().isScanNewlyAddedTableEnabled()) {
            // the new added sharding table without history records
            return !maxSplitHighWatermarkMap.containsKey(tableId)
                    && capturedTableFilter.isIncluded(tableId);
        }
        return false;
    }

    private void configureFilter() {
        List<FinishedSnapshotSplitInfo> finishedSplitInfos =
                currentBinlogSplit.getFinishedSnapshotSplitInfos();
        Map<TableId, List<FinishedSnapshotSplitInfo>> splitsInfoMap = new HashMap<>();
        Map<TableId, BinlogOffset> tableIdBinlogPositionMap = new HashMap<>();
        // specific offset mode
        if (finishedSplitInfos.isEmpty()) {
            for (TableId tableId : currentBinlogSplit.getTableSchemas().keySet()) {
                tableIdBinlogPositionMap.put(tableId, currentBinlogSplit.getStartingOffset());
            }
        }
        // initial mode
        else {
            for (FinishedSnapshotSplitInfo finishedSplitInfo : finishedSplitInfos) {
                TableId tableId = finishedSplitInfo.getTableId();
                List<FinishedSnapshotSplitInfo> list =
                        splitsInfoMap.getOrDefault(tableId, new ArrayList<>());
                list.add(finishedSplitInfo);
                splitsInfoMap.put(tableId, list);

                BinlogOffset highWatermark = finishedSplitInfo.getHighWatermark();
                BinlogOffset maxHighWatermark = tableIdBinlogPositionMap.get(tableId);
                if (maxHighWatermark == null || highWatermark.isAfter(maxHighWatermark)) {
                    tableIdBinlogPositionMap.put(tableId, highWatermark);
                }
            }
        }
        this.finishedSplitsInfo = splitsInfoMap;
        this.maxSplitHighWatermarkMap = tableIdBinlogPositionMap;
        this.pureBinlogPhaseTables.clear();
    }

    private Predicate<Event> createEventFilter() {
        // If the startup mode is set as TIMESTAMP, we need to apply a filter on event to drop
        // events earlier than the specified timestamp.

        // NOTE: Here we take user's configuration (statefulTaskContext.getSourceConfig())
        // as the ground truth. This might be fragile if user changes the config and recover
        // the job from savepoint / checkpoint, as there might be conflict between user's config
        // and the state in savepoint / checkpoint. But as we don't promise compatibility of
        // checkpoint after changing the config, this is acceptable for now.
        StartupOptions startupOptions = statefulTaskContext.getSourceConfig().getStartupOptions();
        if (startupOptions.startupMode.equals(StartupMode.TIMESTAMP)) {
            if (startupOptions.binlogOffset == null) {
                throw new NullPointerException(
                        "The startup option was set to TIMESTAMP "
                                + "but unable to find starting binlog offset. Please check if the timestamp is specified in "
                                + "configuration. ");
            }
            long startTimestampSec = startupOptions.binlogOffset.getTimestampSec();
            // We only skip data change event, as other kinds of events are necessary for updating
            // some internal state inside MySqlStreamingChangeEventSource
            LOG.info(
                    "Creating event filter that dropping row mutation events before timestamp in second {}",
                    startTimestampSec);
            return event -> {
                if (!EventType.isRowMutation(getEventType(event))) {
                    return true;
                }
                return event.getHeader().getTimestamp() >= startTimestampSec * 1000;
            };
        }
        return event -> true;
    }

    public void stopBinlogReadTask() {
        currentTaskRunning = false;
        // Terminate the while loop in MySqlStreamingChangeEventSource's execute method
        changeEventSourceContext.stopChangeEventSource();
    }

    private EventType getEventType(Event event) {
        return event.getHeader().getEventType();
    }

    @VisibleForTesting
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @VisibleForTesting
    MySqlBinlogSplitReadTask getBinlogSplitReadTask() {
        return binlogSplitReadTask;
    }
}
