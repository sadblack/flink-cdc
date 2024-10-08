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
import org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import org.apache.flink.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import org.apache.flink.cdc.connectors.mysql.debezium.task.MySqlSnapshotSplitReadTask;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A snapshot reader that reads data from Table in split level, the split is assigned by primary key
 * range.
 */
public class SnapshotSplitReader implements DebeziumReader<SourceRecords, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotSplitReader.class);
    private StatefulTaskContext statefulTaskContext;
    private ExecutorService executorService;
    private SnapshotPhaseHooks hooks;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;

    // task to read snapshot for current split
    private MySqlSnapshotSplitReadTask splitSnapshotReadTask;
    private MySqlSnapshotSplit currentSnapshotSplit;
    private SchemaNameAdjuster nameAdjuster;
    public AtomicBoolean hasNextElement;
    public AtomicBoolean reachEnd;

    private final StoppableChangeEventSourceContext changeEventSourceContext =
            new StoppableChangeEventSourceContext();

    private static final long READER_CLOSE_TIMEOUT = 30L;

    public SnapshotSplitReader() {
    }

    public void setStatefulTaskContext(StatefulTaskContext statefulTaskContext) {
        this.statefulTaskContext = statefulTaskContext;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public void setHooks(SnapshotPhaseHooks hooks) {
        this.hooks = hooks;
    }

    public void setCurrentTaskRunning(boolean currentTaskRunning) {
        this.currentTaskRunning = currentTaskRunning;
    }

    public void setHasNextElement(AtomicBoolean hasNextElement) {
        this.hasNextElement = hasNextElement;
    }

    public void setReachEnd(AtomicBoolean reachEnd) {
        this.reachEnd = reachEnd;
    }

    public SnapshotSplitReader(
            StatefulTaskContext statefulTaskContext, int subtaskId, SnapshotPhaseHooks hooks) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("debezium-reader-" + subtaskId)
                        .setUncaughtExceptionHandler(
                                (thread, throwable) -> setReadException(throwable))
                        .build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.hooks = hooks;
        this.currentTaskRunning = false;
        this.hasNextElement = new AtomicBoolean(false);
        this.reachEnd = new AtomicBoolean(false);
    }

    public SnapshotSplitReader(StatefulTaskContext statefulTaskContext, int subtaskId) {
        this(statefulTaskContext, subtaskId, SnapshotPhaseHooks.empty());
    }

    @Override
    public void submitSplit(MySqlSplit mySqlSplit) {
        LOG.info(String.format("开始执行snapshotsplit: %s", mySqlSplit));
        this.currentSnapshotSplit = mySqlSplit.asSnapshotSplit();
        statefulTaskContext.configure(currentSnapshotSplit);
        this.queue = statefulTaskContext.getQueue();
        this.nameAdjuster = statefulTaskContext.getSchemaNameAdjuster();
        this.hasNextElement.set(true);
        this.reachEnd.set(false);
        this.splitSnapshotReadTask =
                new MySqlSnapshotSplitReadTask(
                        statefulTaskContext.getSourceConfig(),
                        statefulTaskContext.getConnectorConfig(),
                        statefulTaskContext.getSnapshotChangeEventSourceMetrics(),
                        statefulTaskContext.getDatabaseSchema(),
                        statefulTaskContext.getConnection(),
                        statefulTaskContext.getDispatcher(),
                        statefulTaskContext.getTopicSelector(),
                        statefulTaskContext.getSnapshotReceiver(),
                        StatefulTaskContext.getClock(),
                        currentSnapshotSplit,
                        hooks,
                        statefulTaskContext.getSourceConfig().isSkipSnapshotBackfill());
        executorService.execute(
                () -> {
                    try {
                        currentTaskRunning = true;
                        final SnapshotSplitChangeEventSourceContextImpl sourceContext =
                                new SnapshotSplitChangeEventSourceContextImpl();

                        // Step 1: execute snapshot read task
                        SnapshotResult<MySqlOffsetContext> snapshotResult = snapshot(sourceContext);

                        // Step 2: read binlog events between low and high watermark and backfill
                        // changes into snapshot
                        backfill(snapshotResult, sourceContext);

                    } catch (Exception e) {
                        setReadException(e);
                    } finally {
                        stopCurrentTask();
                    }
                });
    }

    private SnapshotResult<MySqlOffsetContext> snapshot(
            SnapshotSplitChangeEventSourceContextImpl sourceContext) throws Exception {
        return splitSnapshotReadTask.execute(
                sourceContext,
                statefulTaskContext.getMySqlPartition(),
                statefulTaskContext.getOffsetContext());
    }

    /**
     * 执行回填操作
     * 在快照扫描完成后，根据快照结果和当前的offset情况，决定是否需要进行回填
     * 如果需要回填，则创建回填的binlog读取任务并执行
     * 如果不需要回填，则直接触发BINLOG_END事件并停止当前任务
     *
     * @param snapshotResult 快照扫描的结果，包含快照过程中获取的offset信息
     * @param sourceContext  快照数据源上下文，用于创建回填的binlog split
     * @throws Exception 如果执行过程中出现错误，抛出异常
     */
    private void backfill(
            SnapshotResult<MySqlOffsetContext> snapshotResult,
            SnapshotSplitChangeEventSourceContextImpl sourceContext)
            throws Exception {
        // 创建回填的binlog split
        final MySqlBinlogSplit backfillBinlogSplit = createBackfillBinlogSplit(sourceContext);
        BinlogOffset lowWatermark = sourceContext.getLowWatermark();
        BinlogOffset highWatermark = sourceContext.getHighWatermark();
        LOG.info(String.format("snapshot split 执行完毕\n low watermark: %s\n high watermark: %s\n", lowWatermark, highWatermark));
        // 如果不需要回填，直接触发BINLOG_END事件并停止当前任务
        if (!isBackfillRequired(backfillBinlogSplit)) {
            dispatchBinlogEndEvent(backfillBinlogSplit);
            stopCurrentTask();
            return;
        }

        // 如果快照扫描已经完成或被跳过，则创建并执行回填的binlog读取任务
        if (snapshotResult.isCompletedOrSkipped()) {
            // 创建回填的binlog读取任务
            final MySqlBinlogSplitReadTask backfillBinlogReadTask =
                    createBackfillBinlogReadTask(backfillBinlogSplit);
            // 创建offset加载器，用于从状态中加载offset
            final MySqlOffsetContext.Loader loader =
                    new MySqlOffsetContext.Loader(statefulTaskContext.getConnectorConfig());
            // 加载回填开始时的offset
            final MySqlOffsetContext mySqlOffsetContext =
                    loader.load(backfillBinlogSplit.getStartingOffset().getOffset());

            // 执行回填的binlog读取任务
            backfillBinlogReadTask.execute(
                    changeEventSourceContext,
                    statefulTaskContext.getMySqlPartition(),
                    mySqlOffsetContext);
        } else {
            // 如果快照扫描未完成，则抛出异常
            throw new IllegalStateException(
                    String.format("Read snapshot for mysql split %s fail", currentSnapshotSplit));
        }
    }


    private boolean isBackfillRequired(MySqlBinlogSplit backfillBinlogSplit) {
        return !statefulTaskContext.getSourceConfig().isSkipSnapshotBackfill()
                && backfillBinlogSplit
                        .getEndingOffset()
                        .isAfter(backfillBinlogSplit.getStartingOffset());
    }

    private MySqlBinlogSplit createBackfillBinlogSplit(
            SnapshotSplitChangeEventSourceContextImpl sourceContext) {
        return new MySqlBinlogSplit(
                currentSnapshotSplit.splitId(),
                sourceContext.getLowWatermark(),
                sourceContext.getHighWatermark(),
                new ArrayList<>(),
                currentSnapshotSplit.getTableSchemas(),
                0);
    }

    private MySqlBinlogSplitReadTask createBackfillBinlogReadTask(
            MySqlBinlogSplit backfillBinlogSplit) {
        // we should only capture events for the current table,
        // otherwise, we may can't find corresponding schema
        Configuration dezConf =
                statefulTaskContext
                        .getSourceConfig()
                        .getDbzConfiguration()
                        .edit()
                        .with("table.include.list", currentSnapshotSplit.getTableId().toString())
                        // Disable heartbeat event in snapshot split reader
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();
        // task to read binlog and backfill for current split
        return new MySqlBinlogSplitReadTask(
                new MySqlConnectorConfig(dezConf),
                statefulTaskContext.getConnection(),
                statefulTaskContext.getDispatcher(),
                statefulTaskContext.getSignalEventDispatcher(),
                statefulTaskContext.getErrorHandler(),
                StatefulTaskContext.getClock(),
                statefulTaskContext.getTaskContext(),
                (MySqlStreamingChangeEventSourceMetrics)
                        statefulTaskContext.getStreamingChangeEventSourceMetrics(),
                backfillBinlogSplit,
                event -> true);
    }

    private void dispatchBinlogEndEvent(MySqlBinlogSplit backFillBinlogSplit)
            throws InterruptedException {
        final SignalEventDispatcher signalEventDispatcher =
                new SignalEventDispatcher(
                        statefulTaskContext.getOffsetContext().getOffset(),
                        statefulTaskContext.getTopicSelector().getPrimaryTopic(),
                        statefulTaskContext.getDispatcher().getQueue());
        signalEventDispatcher.dispatchWatermarkEvent(
                backFillBinlogSplit,
                backFillBinlogSplit.getEndingOffset(),
                SignalEventDispatcher.WatermarkKind.BINLOG_END);
    }

    @Override
    public boolean isFinished() {
        return currentSnapshotSplit == null
                || (!currentTaskRunning && !hasNextElement.get() && reachEnd.get());
    }

    @Nullable
    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();

        if (hasNextElement.get()) {
            // data input: [low watermark event][snapshot events][high watermark event][binlog
            // events][binlog-end event]
            // data output: [low watermark event][normalized events][high watermark event]
            boolean reachBinlogStart = false;
            boolean reachBinlogEnd = false;
            SourceRecord lowWatermark = null;
            SourceRecord highWatermark = null;

            Map<Struct, List<SourceRecord>> snapshotRecords = new HashMap<>();
            while (!reachBinlogEnd) {
                checkReadException();
                List<DataChangeEvent> batch = queue.poll();//Debezium 从数据库读出数据后，会放到这个queue里
                for (DataChangeEvent event : batch) {
                    SourceRecord record = event.getRecord();
                    if (lowWatermark == null) {
                        lowWatermark = record;
                        assertLowWatermark(lowWatermark);
                        continue;
                    }

                    if (highWatermark == null && RecordUtils.isHighWatermarkEvent(record)) {
                        highWatermark = record;
                        // snapshot events capture end and begin to capture binlog events
                        reachBinlogStart = true;
                        continue;
                    }

                    if (reachBinlogStart && RecordUtils.isEndWatermarkEvent(record)) {
                        // capture to end watermark events, stop the loop
                        reachBinlogEnd = true;
                        break;
                    }

                    if (!reachBinlogStart) {
                        if (record.key() != null) {
                            snapshotRecords.put(//一个 splits 里的数据先放到 snapshotRecords 里
                                    (Struct) record.key(), Collections.singletonList(record));
                        } else {//什么情况下才会有两条相同的记录
                            List<SourceRecord> records =
                                    snapshotRecords.computeIfAbsent(
                                            (Struct) record.value(), key -> new LinkedList<>());
                            records.add(record);
                        }
                    } else {
                        RecordUtils.upsertBinlog(
                                snapshotRecords,
                                record,
                                currentSnapshotSplit.getSplitKeyType(),
                                nameAdjuster,
                                currentSnapshotSplit.getSplitStart(),
                                currentSnapshotSplit.getSplitEnd());
                    }
                }
            }
            // snapshot split return its data once
            hasNextElement.set(false);

            final List<SourceRecord> normalizedRecords = new ArrayList<>();
            normalizedRecords.add(lowWatermark);
            normalizedRecords.addAll(
                    RecordUtils.formatMessageTimestamp(
                            snapshotRecords.values().stream()
                                    .flatMap(Collection::stream)
                                    .collect(Collectors.toList())));
            normalizedRecords.add(highWatermark);

            final List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(normalizedRecords));
            return sourceRecordsSet.iterator();
        }
        // the data has been polled, no more data
        reachEnd.compareAndSet(false, true);
        return null;
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentSnapshotSplit, readException.getMessage()),
                    readException);
        }
    }

    private void assertLowWatermark(SourceRecord lowWatermark) {
        Preconditions.checkState(
                RecordUtils.isLowWatermarkEvent(lowWatermark),
                String.format(
                        "The first record should be low watermark signal event, but actual is %s",
                        lowWatermark));
    }

    public void setReadException(Throwable throwable) {
        stopCurrentTask();
        LOG.error(
                String.format(
                        "Execute snapshot read task for mysql split %s fail", currentSnapshotSplit),
                throwable);
        if (readException == null) {
            readException = throwable;
        } else {
            readException.addSuppressed(throwable);
        }
    }

    @Override
    public void close() {
        try {
            stopCurrentTask();
            if (statefulTaskContext.getConnection() != null) {
                statefulTaskContext.getConnection().close();
            }
            if (statefulTaskContext.getBinaryLogClient() != null) {
                statefulTaskContext.getBinaryLogClient().disconnect();
            }
            if (statefulTaskContext.getDatabaseSchema() != null) {
                statefulTaskContext.getDatabaseSchema().close();
            }
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the snapshot split reader in {} seconds.",
                            READER_CLOSE_TIMEOUT);
                }
            }
        } catch (Exception e) {
            LOG.error("Close snapshot reader error", e);
        }
    }

    private void stopCurrentTask() {
        currentTaskRunning = false;
        changeEventSourceContext.stopChangeEventSource();
    }

    @VisibleForTesting
    public ExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high
     * watermark for each {@link MySqlSnapshotSplit}.
     */
    public static class SnapshotSplitChangeEventSourceContextImpl
            implements ChangeEventSource.ChangeEventSourceContext {

        private BinlogOffset lowWatermark;
        private BinlogOffset highWatermark;

        public BinlogOffset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(BinlogOffset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public BinlogOffset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(BinlogOffset highWatermark) {
            this.highWatermark = highWatermark;
        }

        @Override
        public boolean isRunning() {
            return lowWatermark != null && highWatermark != null;
        }
    }
}
