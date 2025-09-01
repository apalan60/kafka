/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.transaction;

import kafka.coordinator.transaction.TransactionLog;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;
import static org.apache.kafka.server.common.TransactionVersion.TV_0;
import static org.apache.kafka.server.common.TransactionVersion.TV_2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TransactionLogTest {

    private final short producerEpoch = 0;
    private final int transactionTimeoutMs = 1000;

    private final Set<TopicPartition> topicPartitions = Set.of(
        new TopicPartition("topic1", 0),
        new TopicPartition("topic1", 1),
        new TopicPartition("topic2", 0),
        new TopicPartition("topic2", 1),
        new TopicPartition("topic2", 2)
    );

    @Test
    void shouldThrowExceptionWriteInvalidTxn() {
        var transactionalId = "transactionalId";
        var producerId = 23423L;

        var txnMetadata = new TransactionMetadata(transactionalId, producerId, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_ID, producerEpoch,
            RecordBatch.NO_PRODUCER_EPOCH, transactionTimeoutMs, TransactionState.EMPTY, Set.of(), 0, 0, TV_0);
        txnMetadata.addPartitions(topicPartitions);

        var preparedMetadata = txnMetadata.prepareNoTransit();
        assertThrows(IllegalStateException.class, () -> TransactionLog.valueToBytes(preparedMetadata, TV_2));
    }

    @Test
    void shouldReadWriteMessages() {
        var pidMappings = Map.of(
            "zero", 0L,
            "one", 1L,
            "two", 2L,
            "three", 3L,
            "four", 4L,
            "five", 5L
        );

        var transactionStates = Map.of(
            0L, TransactionState.EMPTY,
            1L, TransactionState.ONGOING,
            2L, TransactionState.PREPARE_COMMIT,
            3L, TransactionState.COMPLETE_COMMIT,
            4L, TransactionState.PREPARE_ABORT,
            5L, TransactionState.COMPLETE_ABORT
        );

        // generate transaction log messages
        List<SimpleRecord> txnRecords = new ArrayList<>();
        for (var entry : pidMappings.entrySet()) {

            var txnMetadata = new TransactionMetadata(entry.getKey(), entry.getValue(), RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_ID, producerEpoch,
                RecordBatch.NO_PRODUCER_EPOCH, transactionTimeoutMs, transactionStates.get(entry.getValue()), Set.of(), 0, 0, TV_0);

            if (!txnMetadata.state().equals(TransactionState.EMPTY)) {
                txnMetadata.addPartitions(topicPartitions);
            }

            var keyBytes = TransactionLog.keyToBytes(entry.getKey());
            var valueBytes = TransactionLog.valueToBytes(txnMetadata.prepareNoTransit(), TV_2);

            txnRecords.add(new SimpleRecord(keyBytes, valueBytes));
        }

        var records = MemoryRecords.withRecords(0, Compression.NONE, txnRecords.toArray(new SimpleRecord[0]));

        var count = 0;
        for (var record : records.records()) {
            var keyResult = readTxnRecordKey(record.key());
            if (keyResult instanceof TxnKeyResult.UnknownVersion unknownVersion) {
                fail("Unexpected record version: " + unknownVersion.version());
            } else if (keyResult instanceof TxnKeyResult.TransactionalId transactionalIdResult) {
                var txnMetadata = TransactionLog.readTxnRecordValue(transactionalIdResult.id(), record.value()).get();

                assertEquals(pidMappings.get(transactionalIdResult.id()), txnMetadata.producerId());
                assertEquals(producerEpoch, txnMetadata.producerEpoch());
                assertEquals(transactionTimeoutMs, txnMetadata.txnTimeoutMs());
                assertEquals(transactionStates.get(txnMetadata.producerId()), txnMetadata.state());

                if (txnMetadata.state().equals(TransactionState.EMPTY)) {
                    assertEquals(Set.of(), txnMetadata.topicPartitions());
                } else {
                    assertEquals(topicPartitions, txnMetadata.topicPartitions());
                }

                count++;
            }
        }

        assertEquals(pidMappings.size(), count);
    }

    @Test
    void testSerializeTransactionLogValueToHighestNonFlexibleVersion() {
        var txnTransitMetadata = new TxnTransitMetadata(1L, 1L, 1L, (short) 1, (short) 1, 1000, TransactionState.COMPLETE_COMMIT, new HashSet<>(), 500L, 500L, TV_0);
        var txnLogValueBuffer = ByteBuffer.wrap(TransactionLog.valueToBytes(txnTransitMetadata, TV_0));
        assertEquals(TV_0.transactionLogValueVersion(), txnLogValueBuffer.getShort());
    }

    @Test
    void testSerializeTransactionLogValueToFlexibleVersion() {
        var txnTransitMetadata = new TxnTransitMetadata(1L, 1L, 1L, (short) 1, (short) 1, 1000, TransactionState.COMPLETE_COMMIT, new HashSet<>(), 500L, 500L, TV_2);
        var txnLogValueBuffer = ByteBuffer.wrap(TransactionLog.valueToBytes(txnTransitMetadata, TV_2));
        assertEquals(TransactionLogValue.HIGHEST_SUPPORTED_VERSION, txnLogValueBuffer.getShort());
    }

    @Test
    void testDeserializeHighestSupportedTransactionLogValue() {
        var txnPartitions = new TransactionLogValue.PartitionsSchema()
            .setTopic("topic")
            .setPartitionIds(List.of(0));

        var txnLogValue = new TransactionLogValue()
            .setProducerId(100)
            .setProducerEpoch((short) 50)
            .setTransactionStatus(TransactionState.COMPLETE_COMMIT.id())
            .setTransactionStartTimestampMs(750L)
            .setTransactionLastUpdateTimestampMs(1000L)
            .setTransactionTimeoutMs(500)
            .setTransactionPartitions(List.of(txnPartitions));

        var serialized = MessageUtil.toVersionPrefixedByteBuffer((short) 1, txnLogValue);
        var deserialized = TransactionLog.readTxnRecordValue("transactionId", serialized).get();

        assertEquals(100, deserialized.producerId());
        assertEquals(50, deserialized.producerEpoch());
        assertEquals(TransactionState.COMPLETE_COMMIT, deserialized.state());
        assertEquals(750L, deserialized.txnStartTimestamp());
        assertEquals(1000L, deserialized.txnLastUpdateTimestamp());
        assertEquals(500, deserialized.txnTimeoutMs());

        var actualTxnPartitions = deserialized.topicPartitions();
        assertEquals(1, actualTxnPartitions.size());
        assertTrue(actualTxnPartitions.contains(new TopicPartition("topic", 0)));
    }

    @Test
    void testDeserializeFutureTransactionLogValue() {
        // Copy of TransactionLogValue.PartitionsSchema.SCHEMA_1 with a few
        // additional tagged fields.
        var futurePartitionsSchema = new Schema(
            new Field("topic", Type.COMPACT_STRING, ""),
            new Field("partition_ids", new CompactArrayOf(Type.INT32), ""),
            TaggedFieldsSection.of(
                100, new Field("partition_foo", Type.STRING, ""),
                101, new Field("partition_foo", Type.INT32, "")
            )
        );

        // Create TransactionLogValue.PartitionsSchema with tagged fields
        var txnPartitions = new Struct(futurePartitionsSchema);
        txnPartitions.set("topic", "topic");
        txnPartitions.set("partition_ids", new Integer[]{1});
        var txnPartitionsTaggedFields = new TreeMap<Integer, Object>();
        txnPartitionsTaggedFields.put(100, "foo");
        txnPartitionsTaggedFields.put(101, 4000);
        txnPartitions.set("_tagged_fields", txnPartitionsTaggedFields);

        // Copy of TransactionLogValue.SCHEMA_1 with a few
        // additional tagged fields.
        var futureTransactionLogValueSchema = new Schema(
            new Field("producer_id", Type.INT64, ""),
            new Field("producer_epoch", Type.INT16, ""),
            new Field("transaction_timeout_ms", Type.INT32, ""),
            new Field("transaction_status", Type.INT8, ""),
            new Field("transaction_partitions", CompactArrayOf.nullable(futurePartitionsSchema), ""),
            new Field("transaction_last_update_timestamp_ms", Type.INT64, ""),
            new Field("transaction_start_timestamp_ms", Type.INT64, ""),
            TaggedFieldsSection.of(
                100, new Field("txn_foo", Type.STRING, ""),
                101, new Field("txn_bar", Type.INT32, "")
            )
        );

        // Create TransactionLogValue with tagged fields
        var transactionLogValue = new Struct(futureTransactionLogValueSchema);
        transactionLogValue.set("producer_id", 1000L);
        transactionLogValue.set("producer_epoch", (short) 100);
        transactionLogValue.set("transaction_timeout_ms", 1000);
        transactionLogValue.set("transaction_status", TransactionState.COMPLETE_COMMIT.id());
        transactionLogValue.set("transaction_partitions", new Struct[]{txnPartitions});
        transactionLogValue.set("transaction_last_update_timestamp_ms", 2000L);
        transactionLogValue.set("transaction_start_timestamp_ms", 3000L);
        var txnLogValueTaggedFields = new TreeMap<Integer, Object>();
        txnLogValueTaggedFields.put(100, "foo");
        txnLogValueTaggedFields.put(101, 4000);
        transactionLogValue.set("_tagged_fields", txnLogValueTaggedFields);

        // Prepare the buffer.
        var buffer = ByteBuffer.allocate(transactionLogValue.sizeOf() + 2);
        buffer.put((byte) 0);
        buffer.put((byte) 1); // Add 1 as version.
        transactionLogValue.writeTo(buffer);
        buffer.flip();

        // Read the buffer with the real schema and verify that tagged
        // fields were read but ignored.
        buffer.getShort(); // Skip version.
        var value = new TransactionLogValue(new ByteBufferAccessor(buffer), (short) 1);
        assertEquals(List.of(100, 101), value.unknownTaggedFields().stream().map(RawTaggedField::tag).toList());
        assertEquals(List.of(100, 101), value.transactionPartitions().get(0).unknownTaggedFields().stream().map(RawTaggedField::tag).toList());

        // Read the buffer with readTxnRecordValue.
        buffer.rewind();
        var txnMetadata = TransactionLog.readTxnRecordValue("transaction-id", buffer);
        
        if (txnMetadata.isEmpty()) {
            fail("Expected transaction metadata but got none");
        }

        var metadata = txnMetadata.get();
        assertEquals(1000L, metadata.producerId());
        assertEquals(100, metadata.producerEpoch());
        assertEquals(1000L, metadata.txnTimeoutMs());
        assertEquals(TransactionState.COMPLETE_COMMIT, metadata.state());
        assertEquals(Set.of(new TopicPartition("topic", 1)), metadata.topicPartitions());
        assertEquals(2000L, metadata.txnLastUpdateTimestamp());
        assertEquals(3000L, metadata.txnStartTimestamp());
    }

    @Test
    void testReadTxnRecordKeyCanReadUnknownMessage() {
        var unknownRecord = MessageUtil.toVersionPrefixedBytes(Short.MAX_VALUE, new TransactionLogKey());
        var result = readTxnRecordKey(ByteBuffer.wrap(unknownRecord));
        
        if (result instanceof TxnKeyResult.UnknownVersion unknownVersion) {
            assertEquals(Short.MAX_VALUE, unknownVersion.version());
        } else if (result instanceof TxnKeyResult.TransactionalId) {
            fail("Expected to read unknown message");
        }
    }

    private sealed interface TxnKeyResult {
        record UnknownVersion(short version) implements TxnKeyResult { }
        record TransactionalId(String id) implements TxnKeyResult { }
    }

    private static TxnKeyResult readTxnRecordKey(ByteBuffer buf) {
        var e = TransactionLog.readTxnRecordKey(buf); 
        return e.isLeft()
            ? new TxnKeyResult.UnknownVersion((Short) e.left().get())
            : new TxnKeyResult.TransactionalId(e.right().get());
    }  
}