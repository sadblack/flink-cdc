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

package org.apache.flink.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitAssignedEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetaEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitMetaRequestEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitUpdateAckEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.BinlogSplitUpdateRequestEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsReportEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsRequestEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.LatestFinishedSplitsNumberEvent;
import org.apache.flink.cdc.connectors.mysql.source.events.LatestFinishedSplitsNumberRequestEvent;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.source.assigners.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;

/**
 * A MySQL CDC source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
@Internal
public class MySqlSourceEnumerator implements SplitEnumerator<MySqlSplit, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceEnumerator.class);
    private static final long CHECK_EVENT_INTERVAL = 30_000L;

    private SplitEnumeratorContext<MySqlSplit> context;
    private MySqlSourceConfig sourceConfig;
    private MySqlSplitAssigner splitAssigner;

    private Boundedness boundedness;

    // using TreeSet to prefer assigning binlog split to task-0 for easier debug
    private TreeSet<Integer> readersAwaitingSplit;
    private List<List<FinishedSnapshotSplitInfo>> binlogSplitMeta;

    @Nullable private Integer binlogSplitTaskId;


    public MySqlSourceEnumerator() {
    }

    public void setContext(SplitEnumeratorContext<MySqlSplit> context) {
        this.context = context;
    }

    public void setSourceConfig(MySqlSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public void setSplitAssigner(MySqlSplitAssigner splitAssigner) {
        this.splitAssigner = splitAssigner;
    }

    public void setBoundedness(Boundedness boundedness) {
        this.boundedness = boundedness;
    }

    public void setReadersAwaitingSplit(TreeSet<Integer> readersAwaitingSplit) {
        this.readersAwaitingSplit = readersAwaitingSplit;
    }

    public MySqlSourceEnumerator(
            SplitEnumeratorContext<MySqlSplit> context,
            MySqlSourceConfig sourceConfig,
            MySqlSplitAssigner splitAssigner,
            Boundedness boundedness) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.splitAssigner = splitAssigner;
        this.boundedness = boundedness;
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void start() {
        splitAssigner.open();
        requestBinlogSplitUpdateIfNeed();
        this.context.callAsync(
                this::getRegisteredReader,
                this::syncWithReaders,
                CHECK_EVENT_INTERVAL,
                CHECK_EVENT_INTERVAL);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<MySqlSplit> splits, int subtaskId) {
        LOG.debug("The enumerator adds splits back: {}", splits);
        Optional<MySqlSplit> binlogSplit =
                splits.stream().filter(MySqlSplit::isBinlogSplit).findAny();
        if (binlogSplit.isPresent()) {
            LOG.info("The enumerator adds add binlog split back: {}", binlogSplit);
            this.binlogSplitTaskId = null;
        }
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // send BinlogSplitUpdateRequestEvent to source reader after newly added table
        // snapshot splits finished.
        if (isNewlyAddedAssigningSnapshotFinished(splitAssigner.getAssignerStatus())) {
            context.sendEventToSourceReader(subtaskId, new BinlogSplitUpdateRequestEvent());
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsReportEvent) {
            LOG.info(
                    "The enumerator under {} receives finished split offsets {} from subtask {}.",
                    splitAssigner.getAssignerStatus(),
                    sourceEvent,
                    subtaskId);
            FinishedSnapshotSplitsReportEvent reportEvent =
                    (FinishedSnapshotSplitsReportEvent) sourceEvent;
            Map<String, BinlogOffset> finishedOffsets = reportEvent.getFinishedOffsets();

            splitAssigner.onFinishedSplits(finishedOffsets);
            requestBinlogSplitUpdateIfNeed();

            // send acknowledge event
            FinishedSnapshotSplitsAckEvent ackEvent =
                    new FinishedSnapshotSplitsAckEvent(new ArrayList<>(finishedOffsets.keySet()));
            context.sendEventToSourceReader(subtaskId, ackEvent);
        } else if (sourceEvent instanceof BinlogSplitMetaRequestEvent) {
            LOG.debug(
                    "The enumerator receives request for binlog split meta from subtask {}.",
                    subtaskId);
            sendBinlogMeta(subtaskId, (BinlogSplitMetaRequestEvent) sourceEvent);
        } else if (sourceEvent instanceof BinlogSplitUpdateAckEvent) {
            LOG.info(
                    "The enumerator receives event that the binlog split has been updated from subtask {}. ",
                    subtaskId);
            splitAssigner.onBinlogSplitUpdated();
        } else if (sourceEvent instanceof LatestFinishedSplitsNumberRequestEvent) {
            LOG.info(
                    "The enumerator receives request from subtask {} for the latest finished splits number after added newly tables. ",
                    subtaskId);
            handleLatestFinishedSplitNumberRequest(subtaskId);
        } else if (sourceEvent instanceof BinlogSplitAssignedEvent) {
            LOG.info(
                    "The enumerator receives notice from subtask {} for the binlog split assignment. ",
                    subtaskId);
            binlogSplitTaskId = subtaskId;
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        splitAssigner.notifyCheckpointComplete(checkpointId);
        // binlog split may be available after checkpoint complete
        assignSplits();
    }

    @Override
    public void close() {
        LOG.info("Closing enumerator...");
        splitAssigner.close();
    }

    // ------------------------------------------------------------------------------------------

    /**
     * 分配数据分片给等待的读取器
     * 这个方法遍历所有等待分配数据分片的读取器，并尝试为每个读取器分配一个数据分片
     * 如果读取器不再注册或者处于空闲状态，它将被移除出等待队列
     * 如果有可用的数据分片，它将被分配给读取器，否则请求更新binlog分片
     */
    private void assignSplits() {
        // 创建一个迭代器用于遍历等待分配数据分片的读取器
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        // 遍历所有等待分配数据分片的读取器
        while (awaitingReader.hasNext()) {
            // 获取下一个等待分配数据分片的 splitReader id
            int nextAwaiting = awaitingReader.next();
            // 如果读取器不再注册，移除它并继续下一个循环
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            // 如果满足关闭空闲读取器的条件，发送无更多分片信号，移除它并记录日志
            if (shouldCloseIdleReader(nextAwaiting)) {
                context.signalNoMoreSplits(nextAwaiting);
                awaitingReader.remove();
                LOG.info("Close idle reader of subtask {}", nextAwaiting);
                continue;
            }

            // 尝试获取下一个数据分片
            Optional<MySqlSplit> split = splitAssigner.getNext();
            if (split.isPresent()) {
                // 如果有可用的数据分片，分配给读取器
                final MySqlSplit mySqlSplit = split.get();
                context.assignSplit(mySqlSplit, nextAwaiting);
                // 如果分配的是binlog分片，更新binlog分片任务ID
                if (mySqlSplit instanceof MySqlBinlogSplit) {
                    this.binlogSplitTaskId = nextAwaiting;
                }
                // 移除已经分配数据分片的读取器
                awaitingReader.remove();
                // 记录分配数据分片的日志
                LOG.info("The enumerator assigns split {} to subtask {}", mySqlSplit, nextAwaiting);
            } else {
                // 如果没有可用的数据分片，请求更新binlog分片并结束循环
                requestBinlogSplitUpdateIfNeed();
                break;
            }
        }
    }


    private boolean shouldCloseIdleReader(int nextAwaiting) {
        // When no unassigned split anymore, Signal NoMoreSplitsEvent to awaiting reader in two
        // situations:
        // 1. When Set StartupMode = snapshot mode(also bounded), there's no more splits in the
        // assigner.
        // 2. When set scan.incremental.close-idle-reader.enabled = true, there's no more splits in
        // the assigner.
        return splitAssigner.noMoreSplits()
                && (boundedness == Boundedness.BOUNDED
                        || (sourceConfig.isCloseIdleReaders()
                                && (binlogSplitTaskId != null
                                        && !binlogSplitTaskId.equals(nextAwaiting))));
    }

    private int[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    private void syncWithReaders(int[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to:", t);
        }
        // when the SourceEnumerator restores or the communication failed between
        // SourceEnumerator and SourceReader, it may missed some notification event.
        // tell all SourceReader(s) to report there finished but unacked splits.
        if (splitAssigner.waitingForFinishedSplits()) {
            for (int subtaskId : subtaskIds) {
                context.sendEventToSourceReader(
                        subtaskId, new FinishedSnapshotSplitsRequestEvent());
            }
        }

        requestBinlogSplitUpdateIfNeed();
    }

    private void requestBinlogSplitUpdateIfNeed() {
        if (isNewlyAddedAssigningSnapshotFinished(splitAssigner.getAssignerStatus())) {
            for (int subtaskId : getRegisteredReader()) {
                LOG.info(
                        "The enumerator requests subtask {} to update the binlog split after newly added table.",
                        subtaskId);
                context.sendEventToSourceReader(subtaskId, new BinlogSplitUpdateRequestEvent());
            }
        }
    }

    private void sendBinlogMeta(int subTask, BinlogSplitMetaRequestEvent requestEvent) {
        // initialize once
        if (binlogSplitMeta == null) {
            final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos =
                    splitAssigner.getFinishedSplitInfos();
            if (finishedSnapshotSplitInfos.isEmpty()) {
                LOG.error(
                        "The assigner offers empty finished split information, this should not happen");
                throw new FlinkRuntimeException(
                        "The assigner offers empty finished split information, this should not happen");
            }
            binlogSplitMeta =
                    Lists.partition(
                            finishedSnapshotSplitInfos, sourceConfig.getSplitMetaGroupSize());
        }
        final int requestMetaGroupId = requestEvent.getRequestMetaGroupId();
        final int totalFinishedSplitSizeOfReader = requestEvent.getTotalFinishedSplitSize();
        final int totalFinishedSplitSizeOfEnumerator = splitAssigner.getFinishedSplitInfos().size();
        if (totalFinishedSplitSizeOfReader > totalFinishedSplitSizeOfEnumerator) {
            LOG.warn(
                    "Total finished split size of subtask {} is {}, while total finished split size of Enumerator is only {}. Try to truncate it",
                    subTask,
                    totalFinishedSplitSizeOfReader,
                    totalFinishedSplitSizeOfEnumerator);
            BinlogSplitMetaEvent metadataEvent =
                    new BinlogSplitMetaEvent(
                            requestEvent.getSplitId(),
                            requestMetaGroupId,
                            null,
                            totalFinishedSplitSizeOfEnumerator);
            context.sendEventToSourceReader(subTask, metadataEvent);
        } else if (binlogSplitMeta.size() > requestMetaGroupId) {
            List<FinishedSnapshotSplitInfo> metaToSend = binlogSplitMeta.get(requestMetaGroupId);
            BinlogSplitMetaEvent metadataEvent =
                    new BinlogSplitMetaEvent(
                            requestEvent.getSplitId(),
                            requestMetaGroupId,
                            metaToSend.stream()
                                    .map(FinishedSnapshotSplitInfo::serialize)
                                    .collect(Collectors.toList()),
                            totalFinishedSplitSizeOfEnumerator);
            context.sendEventToSourceReader(subTask, metadataEvent);
        } else {
            throw new FlinkRuntimeException(
                    String.format(
                            "The enumerator received invalid request meta group id %s, the valid meta group id range is [0, %s]. Total finished split size of reader is %s, while the total finished split size of enumerator is %s.",
                            requestMetaGroupId,
                            binlogSplitMeta.size() - 1,
                            totalFinishedSplitSizeOfReader,
                            totalFinishedSplitSizeOfEnumerator));
        }
    }

    private void handleLatestFinishedSplitNumberRequest(int subTask) {
        if (splitAssigner instanceof MySqlHybridSplitAssigner) {
            context.sendEventToSourceReader(
                    subTask,
                    new LatestFinishedSplitsNumberEvent(
                            splitAssigner.getFinishedSplitInfos().size()));
        }
    }
}
