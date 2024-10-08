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

package org.apache.flink.cdc.connectors.mysql.source.reader;

import org.apache.flink.cdc.connectors.mysql.CustomProxy;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlRecords;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;

/** The {@link SplitReader} implementation for the {@link MySqlSource}. */
public class MySqlSplitReader implements SplitReader<SourceRecords, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSplitReader.class);
    private final ArrayDeque<MySqlSnapshotSplit> snapshotSplits;
    private final ArrayDeque<MySqlBinlogSplit> binlogSplits;
    private final MySqlSourceConfig sourceConfig;
    private final int subtaskId;
    private final MySqlSourceReaderContext context;

    private final SnapshotPhaseHooks snapshotHooks;

    @Nullable private String currentSplitId;
    @Nullable private DebeziumReader<SourceRecords, MySqlSplit> currentReader;
    @Nullable private SnapshotSplitReader reusedSnapshotReader;
    @Nullable private BinlogSplitReader reusedBinlogReader;

    public MySqlSplitReader(
            MySqlSourceConfig sourceConfig,
            int subtaskId,
            MySqlSourceReaderContext context,
            SnapshotPhaseHooks snapshotHooks) {
        this.sourceConfig = sourceConfig;
        this.subtaskId = subtaskId;
        this.snapshotSplits = new ArrayDeque<>();
        this.binlogSplits = new ArrayDeque<>(1);
        this.context = context;
        this.snapshotHooks = snapshotHooks;
    }

    @Override
    public RecordsWithSplitIds<SourceRecords> fetch() throws IOException {//SourceRecords 就是 stream 里的数据，fetchTask 会调用这个方法，往 elementQueue 里放数据
        try {
            suspendBinlogReaderIfNeed();
            return pollSplitRecords();
        } catch (InterruptedException e) {
            LOG.warn("fetch data failed.", e);
            throw new IOException(e);
        }
    }

    /** Suspends binlog reader until updated binlog split join again. */
    private void suspendBinlogReaderIfNeed() {
        if (currentReader != null
                && currentReader instanceof BinlogSplitReader
                && context.isBinlogSplitReaderSuspended()
                && !currentReader.isFinished()) {
            ((BinlogSplitReader) currentReader).stopBinlogReadTask();
            LOG.info("Suspend binlog reader to wait the binlog split update.");
        }
    }
    //MySqlRecords 里就是读取并混合封装好的数据
    private MySqlRecords pollSplitRecords() throws InterruptedException {
        Iterator<SourceRecords> dataIt;
        //currentReader 什么时候设置
        if (currentReader == null) {
            // (1) Reads binlog split firstly and then read snapshot split
            if (binlogSplits.size() > 0) {
                // the binlog split may come from:
                // (a) the initial binlog split
                // (b) added back binlog-split in newly added table process
                MySqlSplit nextSplit = binlogSplits.poll();
                currentSplitId = nextSplit.splitId();
                currentReader = getBinlogSplitReader();
                //让当前的reader去处理这个split，reader会创建task，并运行这个task，这是一个异步操作吗
                //这是一个异步操作，所以下面的 直接 poll，可能不会返回数据
                currentReader.submitSplit(nextSplit);
            } else if (snapshotSplits.size() > 0) {
                MySqlSplit nextSplit = snapshotSplits.poll();
                currentSplitId = nextSplit.splitId();
                currentReader = getSnapshotSplitReader();
                //让当前的reader去处理这个split，reader会创建task，并运行这个task，
                //这是一个异步操作，所以下面的 直接 poll，可能不会返回数据
                currentReader.submitSplit(nextSplit);
            } else {
                LOG.info("No available split to read.");
            }
            dataIt = currentReader.pollSplitRecords();
            return dataIt == null ? finishedSplit() : forRecords(dataIt);
        } else if (currentReader instanceof SnapshotSplitReader) {//代表正在运行的是 SnapshotSplitReader
            // (2) try to switch to binlog split reading util current snapshot split finished
            dataIt = currentReader.pollSplitRecords();
            if (dataIt != null) {
                // first fetch data of snapshot split, return and emit the records of snapshot split
                MySqlRecords records;
                if (context.isHasAssignedBinlogSplit()) {
                    records = forNewAddedTableFinishedSplit(currentSplitId, dataIt);
                    closeSnapshotReader();
                    closeBinlogReader();
                } else {
                    records = forRecords(dataIt);
                    MySqlSplit nextSplit = snapshotSplits.poll();
                    if (nextSplit != null) {
                        currentSplitId = nextSplit.splitId();
                        currentReader.submitSplit(nextSplit);
                    } else {
                        closeSnapshotReader();
                    }
                }
                return records;
            } else {//因为是异步操作，为什么是 null 的时候就代表这个已经完成了？ 可能是那边会发通知，然后这边再 poll，poll不到代表结束了
                return finishedSplit();
            }
        } else if (currentReader instanceof BinlogSplitReader) {//什么时候创建 binlogSplitReader
            // (3) switch to snapshot split reading if there are newly added snapshot splits
            dataIt = currentReader.pollSplitRecords();
            if (dataIt != null) {
                // try to switch to read snapshot split if there are new added snapshot
                MySqlSplit nextSplit = snapshotSplits.poll();
                if (nextSplit != null) {
                    closeBinlogReader();
                    LOG.info("It's turn to switch next fetch reader to snapshot split reader");
                    currentSplitId = nextSplit.splitId();
                    currentReader = getSnapshotSplitReader();
                    currentReader.submitSplit(nextSplit);
                }
                return MySqlRecords.forBinlogRecords(BINLOG_SPLIT_ID, dataIt);
            } else {
                // null will be returned after receiving suspend binlog event
                // finish current binlog split reading
                closeBinlogReader();
                return finishedSplit();
            }
        } else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    private MySqlRecords finishedSplit() {
        final MySqlRecords finishedRecords = MySqlRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }

    /**
     * 把 Iterator<SourceRecords> dataIt 封装进 MySqlRecords 里
     *
     * @param dataIt 数据迭代器，包含待处理的源记录
     * @return 返回生成的MySqlRecords对象
     */
    private MySqlRecords forRecords(Iterator<SourceRecords> dataIt) {
        // 判断当前读取器是否为快照读取器
        if (currentReader instanceof SnapshotSplitReader) {
            // 如果是快照读取器，创建并返回快照记录类型的MySqlRecords
            // 同时，关闭快照读取器，表示快照数据已经处理完毕
            final MySqlRecords finishedRecords =
                    MySqlRecords.forSnapshotRecords(currentSplitId, dataIt);
            closeSnapshotReader();
            return finishedRecords;
        } else {
            // 如果不是快照读取器，创建并返回增量记录类型的MySqlRecords
            // 增量记录通常用于处理数据库的增量更新
            return MySqlRecords.forBinlogRecords(currentSplitId, dataIt);
        }
    }

    /**
     * Finishes new added snapshot split, mark the binlog split as finished too, we will add the
     * binlog split back in {@code MySqlSourceReader}.
     */
    private MySqlRecords forNewAddedTableFinishedSplit(
            final String splitId, final Iterator<SourceRecords> recordsForSplit) {
        final Set<String> finishedSplits = new HashSet<>();
        finishedSplits.add(splitId);
        finishedSplits.add(BINLOG_SPLIT_ID);
        currentSplitId = null;
        return new MySqlRecords(splitId, recordsForSplit, finishedSplits);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MySqlSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.info("Handling split change {}", splitsChanges);
        for (MySqlSplit mySqlSplit : splitsChanges.splits()) {
            if (mySqlSplit.isSnapshotSplit()) {
                snapshotSplits.add(mySqlSplit.asSnapshotSplit());
            } else {
                binlogSplits.add(mySqlSplit.asBinlogSplit());
            }
        }
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        closeSnapshotReader();
        closeBinlogReader();
    }

    private SnapshotSplitReader getSnapshotSplitReader() {
        if (reusedSnapshotReader == null) {
            final MySqlConnection jdbcConnection =
                    DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                    DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                    new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            SnapshotSplitReader snapshotReader =
                    new SnapshotSplitReader(statefulTaskContext, subtaskId, snapshotHooks);

            snapshotReader = new CustomProxy<>(snapshotReader).getProxy(e -> {
                e.setStatefulTaskContext(statefulTaskContext);
                ThreadFactory threadFactory =
                        new ThreadFactoryBuilder()
                                .setNameFormat("debezium-reader-" + subtaskId)
                                .setUncaughtExceptionHandler(
                                        (thread, throwable) -> e.setReadException(throwable))
                                .build();
                e.setExecutorService(Executors.newSingleThreadExecutor(threadFactory));
                e.setHooks(snapshotHooks);
                e.setCurrentTaskRunning(false);
                e.setHasNextElement(new AtomicBoolean(false));
                e.setReachEnd(new AtomicBoolean(false));
                return e;
            });
            reusedSnapshotReader = snapshotReader;
        }
        return reusedSnapshotReader;
    }

    private BinlogSplitReader getBinlogSplitReader() {
        if (reusedBinlogReader == null) {
            final MySqlConnection jdbcConnection =
                    DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                    DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                    new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            BinlogSplitReader binlogReader = new BinlogSplitReader(statefulTaskContext, subtaskId);
            binlogReader = new CustomProxy<>(binlogReader).getProxy(reader -> {
                reader.setStatefulTaskContext(statefulTaskContext);
                reader.setExecutorService(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("binlog-reader-" + subtaskId).build()));
                reader.setCurrentTaskRunning(true);
                reader.setPureBinlogPhaseTables(new HashSet<>());
                return reader;
            });
            reusedBinlogReader = binlogReader;
        }
        return reusedBinlogReader;
    }

    private void closeSnapshotReader() {
        if (reusedSnapshotReader != null) {
            LOG.debug(
                    "Close snapshot reader {}", reusedSnapshotReader.getClass().getCanonicalName());
            reusedSnapshotReader.close();
            if (reusedSnapshotReader == currentReader) {
                currentReader = null;
            }
            reusedSnapshotReader = null;
        }
    }

    private void closeBinlogReader() {
        if (reusedBinlogReader != null) {
            LOG.debug("Close binlog reader {}", reusedBinlogReader.getClass().getCanonicalName());
            reusedBinlogReader.close();
            if (reusedBinlogReader == currentReader) {
                currentReader = null;
            }
            reusedBinlogReader = null;
        }
    }
}
