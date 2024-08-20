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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.WatermarkKind;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.data.Envelope;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl.HISTORY_RECORD_FIELD;
import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.SIGNAL_EVENT_VALUE_SCHEMA_NAME;
import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.SPLIT_ID_KEY;
import static org.apache.flink.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher.WATERMARK_KIND;

/** Utility class to deal record. */
public class RecordUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RecordUtils.class);

    private RecordUtils() {}

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.mysql.SchemaChangeKey";
    public static final String SCHEMA_HEARTBEAT_EVENT_KEY_NAME =
            "io.debezium.connector.common.Heartbeat";
    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();

    /** Converts a {@link ResultSet} row to an array of Objects. */
    public static Object[] rowToArray(ResultSet rs, int size) throws SQLException {
        final Object[] row = new Object[size];
        for (int i = 0; i < size; i++) {
            row[i] = rs.getObject(i + 1);
        }
        return row;
    }

    public static Struct getStructContainsChunkKey(SourceRecord record) {
        // Use chunk key in the after struct for insert or
        // the before struct for delete/update
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            return value.getStruct(Envelope.FieldName.AFTER);
        } else {
            return value.getStruct(Envelope.FieldName.BEFORE);
        }
    }

    /**
     * 根据binlog记录更新快照记录
     *
     * 此方法主要用于处理数据变更记录（DCR），根据不同的操作类型（CREATE, UPDATE, DELETE, READ），
     * 将binlog记录插入或更新到快照记录中相应的位置如果操作类型是READ，则抛出异常，
     * 因为binlog记录不应该使用READ操作
     *
     * @param snapshotRecords 快照记录，以结构为键，存储一组源记录
     * @param binlogRecord binlog记录，包含数据库变更信息
     * @param splitBoundaryType 分片边界类型，用于定义分片键的类型
     * @param nameAdjuster 名称调整器，用于调整字段名称
     * @param splitStart 分片开始键，定义分片的起始边界
     * @param splitEnd 分片结束键，定义分片的结束边界
     */
    public static void upsertBinlog(
            Map<Struct, List<SourceRecord>> snapshotRecords,
            SourceRecord binlogRecord,
            RowType splitBoundaryType,
            SchemaNameAdjuster nameAdjuster,
            Object[] splitStart,
            Object[] splitEnd) {
        // 检查是否是数据变更记录
        if (isDataChangeRecord(binlogRecord)) {
            // 获取binlog记录的值部分，通常包含变更后的数据
            Struct value = (Struct) binlogRecord.value();
            if (value != null) {
                // 获取包含分块键的结构体
                Struct chunkKeyStruct = getStructContainsChunkKey(binlogRecord);
                // 检查分块键是否在当前分片范围内
                if (splitKeyRangeContains(
                        getSplitKey(splitBoundaryType, nameAdjuster, chunkKeyStruct),
                        splitStart,
                        splitEnd)) {
                    // 检查是否存在主键
                    boolean hasPrimaryKey = binlogRecord.key() != null;
                    // 获取操作类型（CREATE, UPDATE, DELETE, READ）
                    Envelope.Operation operation =
                            Envelope.Operation.forCode(
                                    value.getString(Envelope.FieldName.OPERATION));
                    // 根据操作类型处理记录
                    switch (operation) {
                        case CREATE:
                            // 插入新记录，如果存在主键，则使用主键，否则使用变更后的值
                            upsertBinlog(
                                    snapshotRecords,
                                    binlogRecord,
                                    hasPrimaryKey
                                            ? (Struct) binlogRecord.key()
                                            : createReadOpValue(
                                                    binlogRecord, Envelope.FieldName.AFTER),
                                    false);
                            break;
                        case UPDATE:
                            // 获取变更后的结构体
                            Struct structFromAfter =
                                    createReadOpValue(binlogRecord, Envelope.FieldName.AFTER);
                            // 如果不存在主键，先尝试插入旧主键，再处理分块键变化的情况
                            if (!hasPrimaryKey) {
                                upsertBinlog(
                                        snapshotRecords,
                                        binlogRecord,
                                        createReadOpValue(binlogRecord, Envelope.FieldName.BEFORE),
                                        true);
                                // 如果更新后的分块键超出分片范围，记录警告
                                if (!splitKeyRangeContains(
                                        getSplitKey(
                                                splitBoundaryType, nameAdjuster, structFromAfter),
                                        splitStart,
                                        splitEnd)) {
                                    LOG.warn(
                                            "The updated chunk key is out of the split range. Cannot provide exactly-once semantics.");
                                }
                            }
                            // 无论如何，更新现有记录，这里可能引入至少一次语义
                            upsertBinlog(
                                    snapshotRecords,
                                    binlogRecord,
                                    hasPrimaryKey ? (Struct) binlogRecord.key() : structFromAfter,
                                    false);
                            break;
                        case DELETE:
                            // 删除记录，如果存在主键，则使用主键，否则使用变更前的值
                            upsertBinlog(
                                    snapshotRecords,
                                    binlogRecord,
                                    hasPrimaryKey
                                            ? (Struct) binlogRecord.key()
                                            : createReadOpValue(
                                                    binlogRecord, Envelope.FieldName.BEFORE),
                                    true);
                            break;
                        case READ:
                            // 抛出异常，不应该有READ操作类型的binlog记录
                            throw new IllegalStateException(
                                    String.format(
                                            "Binlog record shouldn't use READ operation, the the record is %s.",
                                            binlogRecord));
                    }
                }
            }
        }
    }


    /**
     * 根据给定的条件将binlog记录插入或更新到快照记录中
     *
     * @param snapshotRecords 快照记录的集合，映射为每个结构化的键提供一个源记录列表
     * @param binlogRecord    待插入或更新的binlog记录
     * @param keyStruct       用于查找快照记录的键结构
     * @param isDelete        表示binlog记录是否为删除操作
     */
    private static void upsertBinlog(
            Map<Struct, List<SourceRecord>> snapshotRecords,
            SourceRecord binlogRecord,
            Struct keyStruct,
            boolean isDelete) {
        // 检查binlog记录是否包含主键
        boolean hasPrimaryKey = binlogRecord.key() != null;
        // 获取与给定键结构关联的源记录列表
        List<SourceRecord> records = snapshotRecords.get(keyStruct);
        // 如果binlog记录表示一个删除操作
        if (isDelete) {
            // 如果记录不存在或者列表为空，记录错误日志
            if (records == null || records.isEmpty()) {
                LOG.error(
                        "Deleting a record which is not in its split for tables without primary keys. This may happen when the chunk key column is updated in another snapshot split.");
            } else if (hasPrimaryKey) {
                // 如果有主键，从快照记录中移除对应的键结构
                snapshotRecords.remove(keyStruct);
            } else {
                // 如果没有主键，移除列表中的第一个记录
                snapshotRecords.get(keyStruct).remove(0);
            }
        } else {
            // 创建一个新的源记录，代表插入或更新操作
            SourceRecord record =
                    new SourceRecord(
                            binlogRecord.sourcePartition(),
                            binlogRecord.sourceOffset(),
                            binlogRecord.topic(),
                            binlogRecord.kafkaPartition(),
                            binlogRecord.keySchema(),
                            binlogRecord.key(),
                            binlogRecord.valueSchema(),
                            createReadOpValue(binlogRecord, Envelope.FieldName.AFTER));
            // 如果有主键，将新记录放入快照记录中
            if (hasPrimaryKey) {
                snapshotRecords.put(keyStruct, Collections.singletonList(record));
            } else {
                // 如果没有主键且记录不存在，初始化一个新的列表并放入快照记录中
                if (records == null) {
                    snapshotRecords.put(keyStruct, new LinkedList<>());
                    records = snapshotRecords.get(keyStruct);
                }
                // 将新记录添加到列表中
                records.add(record);
            }
        }
    }


    private static Struct createReadOpValue(SourceRecord binlogRecord, String beforeOrAfter) {
        Struct value = (Struct) binlogRecord.value();

        Envelope envelope = Envelope.fromSchema(binlogRecord.valueSchema());
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        Struct targetStruct = value.getStruct(beforeOrAfter);
        Instant fetchTs = Instant.ofEpochMilli((Long) source.get(Envelope.FieldName.TIMESTAMP));
        return envelope.read(targetStruct, source, fetchTs);
    }

    /**
     * Format message timestamp(source.ts_ms) value to 0L for all records read in snapshot phase.
     */
    public static List<SourceRecord> formatMessageTimestamp(
            Collection<SourceRecord> snapshotRecords) {
        return snapshotRecords.stream()
                .map(
                        record -> {
                            Envelope envelope = Envelope.fromSchema(record.valueSchema());
                            Struct value = (Struct) record.value();
                            Struct updateAfter = value.getStruct(Envelope.FieldName.AFTER);
                            // set message timestamp (source.ts_ms) to 0L
                            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                            source.put(Envelope.FieldName.TIMESTAMP, 0L);
                            // extend the fetch timestamp(ts_ms)
                            Instant fetchTs =
                                    Instant.ofEpochMilli(
                                            value.getInt64(Envelope.FieldName.TIMESTAMP));
                            SourceRecord sourceRecord =
                                    new SourceRecord(
                                            record.sourcePartition(),
                                            record.sourceOffset(),
                                            record.topic(),
                                            record.kafkaPartition(),
                                            record.keySchema(),
                                            record.key(),
                                            record.valueSchema(),
                                            envelope.read(updateAfter, source, fetchTs));
                            return sourceRecord;
                        })
                .collect(Collectors.toList());
    }

    public static boolean isWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        return watermarkKind.isPresent();
    }

    public static boolean isLowWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        if (watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.LOW) {
            return true;
        }
        return false;
    }

    public static boolean isHighWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        if (watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.HIGH) {
            return true;
        }
        return false;
    }

    public static boolean isEndWatermarkEvent(SourceRecord record) {
        Optional<WatermarkKind> watermarkKind = getWatermarkKind(record);
        if (watermarkKind.isPresent() && watermarkKind.get() == WatermarkKind.BINLOG_END) {
            return true;
        }
        return false;
    }

    public static BinlogOffset getWatermark(SourceRecord watermarkEvent) {
        return getBinlogPosition(watermarkEvent.sourceOffset());
    }

    /**
     * Return the timestamp when the change event is produced in MySQL.
     *
     * <p>The field `source.ts_ms` in {@link SourceRecord} data struct is the time when the change
     * event is operated in MySQL.
     */
    public static Long getMessageTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.SOURCE) == null) {
            return null;
        }

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source.schema().field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }

        return source.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    /**
     * Return the timestamp when the change event is fetched in {@link DebeziumReader}.
     *
     * <p>The field `ts_ms` in {@link SourceRecord} data struct is the time when the record fetched
     * by debezium reader, use it as the process time in Source.
     */
    public static Long getFetchTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }
        return value.getInt64(Envelope.FieldName.TIMESTAMP);
    }

    public static boolean isSchemaChangeEvent(SourceRecord sourceRecord) {
        Schema keySchema = sourceRecord.keySchema();
        if (keySchema != null && SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name())) {
            return true;
        }
        return false;
    }

    public static boolean isHeartbeatEvent(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        return valueSchema != null
                && SCHEMA_HEARTBEAT_EVENT_KEY_NAME.equalsIgnoreCase(valueSchema.name());
    }

    /**
     * Return the finished snapshot split information.
     *
     * @return [splitId, splitStart, splitEnd, highWatermark], the information will be used to
     *     filter binlog events when read binlog of table.
     */
    public static FinishedSnapshotSplitInfo getSnapshotSplitInfo(
            MySqlSnapshotSplit split, SourceRecord highWatermark) {
        Struct value = (Struct) highWatermark.value();
        String splitId = value.getString(SPLIT_ID_KEY);
        return new FinishedSnapshotSplitInfo(
                split.getTableId(),
                splitId,
                split.getSplitStart(),
                split.getSplitEnd(),
                getBinlogPosition(highWatermark.sourceOffset()));
    }

    /** Returns the start offset of the binlog split. */
    public static BinlogOffset getStartingOffsetOfBinlogSplit(
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplits) {
        BinlogOffset startOffset =
                finishedSnapshotSplits.isEmpty()
                        ? BinlogOffset.ofEarliest()
                        : finishedSnapshotSplits.get(0).getHighWatermark();
        for (FinishedSnapshotSplitInfo finishedSnapshotSplit : finishedSnapshotSplits) {
            if (finishedSnapshotSplit.getHighWatermark().isBefore(startOffset)) {
                startOffset = finishedSnapshotSplit.getHighWatermark();
            }
        }
        return startOffset;
    }

    public static boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    public static TableId getTableId(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String dbName = source.getString(DATABASE_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(dbName, null, tableName);
    }

    public static boolean isTableChangeRecord(SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String tableName = source.getString(TABLE_NAME_KEY);
        return !StringUtils.isNullOrWhitespaceOnly(tableName);
    }

    public static Object[] getSplitKey(
            RowType splitBoundaryType, SchemaNameAdjuster nameAdjuster, Struct target) {
        // the split key field contains single field now
        String splitFieldName = nameAdjuster.adjust(splitBoundaryType.getFieldNames().get(0));
        return new Object[] {target.get(splitFieldName)};
    }

    public static BinlogOffset getBinlogPosition(SourceRecord dataRecord) {
        return getBinlogPosition(dataRecord.sourceOffset());
    }

    public static BinlogOffset getBinlogPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return BinlogOffset.builder().setOffsetMap(offsetStrMap).build();
    }

    /** Returns the specific key contains in the split key range or not. */
    public static boolean splitKeyRangeContains(
            Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        // for all range
        if (splitKeyStart == null && splitKeyEnd == null) {
            return true;
        }
        // first split
        if (splitKeyStart == null) {
            int[] upperBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                upperBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
            }
            return Arrays.stream(upperBoundRes).anyMatch(value -> value < 0)
                    && Arrays.stream(upperBoundRes).allMatch(value -> value <= 0);
        }
        // last split
        else if (splitKeyEnd == null) {
            int[] lowerBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
            }
            return Arrays.stream(lowerBoundRes).allMatch(value -> value >= 0);
        }
        // other split
        else {
            int[] lowerBoundRes = new int[key.length];
            int[] upperBoundRes = new int[key.length];
            for (int i = 0; i < key.length; i++) {
                lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
                upperBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
            }
            return Arrays.stream(lowerBoundRes).anyMatch(value -> value >= 0)
                    && (Arrays.stream(upperBoundRes).anyMatch(value -> value < 0)
                            && Arrays.stream(upperBoundRes).allMatch(value -> value <= 0));
        }
    }

    @SuppressWarnings("unchecked")
    private static int compareObjects(Object o1, Object o2) {
        if (o1 instanceof Comparable && o1.getClass().equals(o2.getClass())) {
            return ((Comparable) o1).compareTo(o2);
        } else if (isNumericObject(o1) && isNumericObject(o2)) {
            return toBigDecimal(o1).compareTo(toBigDecimal(o2));
        } else {
            return o1.toString().compareTo(o2.toString());
        }
    }

    private static boolean isNumericObject(Object obj) {
        return obj instanceof Byte
                || obj instanceof Short
                || obj instanceof Integer
                || obj instanceof Long
                || obj instanceof Float
                || obj instanceof Double
                || obj instanceof BigInteger
                || obj instanceof BigDecimal;
    }

    private static BigDecimal toBigDecimal(Object numericObj) {
        return new BigDecimal(numericObj.toString());
    }

    public static HistoryRecord getHistoryRecord(SourceRecord schemaRecord) throws IOException {
        Struct value = (Struct) schemaRecord.value();
        String historyRecordStr = value.getString(HISTORY_RECORD_FIELD);
        return new HistoryRecord(DOCUMENT_READER.read(historyRecordStr));
    }

    private static Optional<WatermarkKind> getWatermarkKind(SourceRecord record) {
        if (record.valueSchema() != null
                && SIGNAL_EVENT_VALUE_SCHEMA_NAME.equals(record.valueSchema().name())) {
            Struct value = (Struct) record.value();
            return Optional.of(WatermarkKind.valueOf(value.getString(WATERMARK_KIND)));
        }
        return Optional.empty();
    }
}
