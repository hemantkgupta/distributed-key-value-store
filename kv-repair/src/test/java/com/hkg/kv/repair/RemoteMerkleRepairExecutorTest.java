package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.partitioning.KeyTokenHasher;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.replication.ReplicaWriter;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StorageEngine;
import com.hkg.kv.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RemoteMerkleRepairExecutorTest {
    private static final ClusterNode LEFT = node("node-left");
    private static final ClusterNode RIGHT = node("node-right");
    private static final TokenRange RANGE = TokenRange.fullRing();
    private static final Instant NOW = Instant.parse("2026-04-22T00:00:00Z");

    @Test
    void streamsRangesAndWritesMissingRecordToRemoteReplica() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        Key key = Key.utf8("user:1");
        left.apply(put(key, "left", 1));
        Map<NodeId, InMemoryStorage> stores = stores(left, right);

        MerkleRepairResult result = executor(stores, successfulWriter(stores)).execute(
                manualPlan(1, 0),
                LEFT,
                RIGHT
        );

        assertThat(result.appliedToRight()).isEqualTo(1);
        assertThat(result.failedWrites()).isZero();
        assertThat(result.fullyApplied()).isTrue();
        assertThat(right.get(key)).contains(left.get(key).orElseThrow());
    }

    @Test
    void streamsRangesAndWritesNewestVersionToStaleRemoteReplica() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        Key key = Key.utf8("user:1");
        left.apply(put(key, "old", 1));
        right.apply(put(key, "new", 2));
        Map<NodeId, InMemoryStorage> stores = stores(left, right);

        MerkleRepairResult result = executor(stores, successfulWriter(stores)).execute(
                manualPlan(1, 1),
                LEFT,
                RIGHT
        );

        assertThat(result.appliedToLeft()).isEqualTo(1);
        assertThat(value(left.get(key).orElseThrow())).isEqualTo("new");
    }

    @Test
    void countsFailedWriteWhenReplicaWriterReturnsWrongNode() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(Key.utf8("user:1"), "left", 1));
        Map<NodeId, InMemoryStorage> stores = stores(left, right);
        ReplicaWriter wrongNodeWriter = (replica, mutation) -> new ReplicaResponse(new NodeId("wrong"), true, "ok");

        MerkleRepairResult result = executor(stores, wrongNodeWriter).execute(manualPlan(1, 0), LEFT, RIGHT);

        assertThat(result.appliedToRight()).isZero();
        assertThat(result.failedWrites()).isEqualTo(1);
        assertThat(result.fullyApplied()).isFalse();
        assertThat(right.scanAll()).isEmpty();
    }

    @Test
    void stopsWithinStreamedRangeWhenWriteBudgetIsExhausted() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(Key.utf8("user:1"), "first", 1));
        left.apply(put(Key.utf8("user:2"), "second", 2));
        Map<NodeId, InMemoryStorage> stores = stores(left, right);

        MerkleRepairResult result = executor(stores, successfulWriter(stores)).execute(
                manualPlan(2, 0),
                LEFT,
                RIGHT,
                new MerkleRepairBudget(10, 10, 1)
        );

        assertThat(result.successfulWrites()).isEqualTo(1);
        assertThat(result.skippedRanges()).isEqualTo(1);
        assertThat(result.stoppedByBudget()).isTrue();
        assertThat(right.scanAll()).hasSize(1);
    }

    @Test
    void rejectsStreamedRecordsOutsideRequestedRange() {
        List<TokenRange> ranges = RANGE.split();
        Key outsideRange = keyInRange(ranges.get(1), "outside");
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(outsideRange, "left", 1));
        Map<NodeId, InMemoryStorage> stores = stores(left, right);
        MerkleRepairPlan plan = new MerkleRepairPlan(
                ranges.get(0),
                List.of(new MerkleDifference(ranges.get(0), 1, 0, new byte[] {1}, new byte[] {2}))
        );

        assertThatThrownBy(() -> executor(stores, successfulWriter(stores)).execute(plan, LEFT, RIGHT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outside the requested range");
    }

    private static RemoteMerkleRepairExecutor executor(
            Map<NodeId, InMemoryStorage> stores,
            ReplicaWriter replicaWriter
    ) {
        MerkleRangeStreamer streamer = (replica, range) -> stores.get(replica.nodeId()).scanAll();
        return new RemoteMerkleRepairExecutor(streamer, replicaWriter);
    }

    private static ReplicaWriter successfulWriter(Map<NodeId, InMemoryStorage> stores) {
        return (replica, mutation) -> {
            stores.get(replica.nodeId()).apply(mutation);
            return new ReplicaResponse(replica.nodeId(), true, "ok");
        };
    }

    private static Map<NodeId, InMemoryStorage> stores(InMemoryStorage left, InMemoryStorage right) {
        return Map.of(LEFT.nodeId(), left, RIGHT.nodeId(), right);
    }

    private static MerkleRepairPlan manualPlan(int leftRecordCount, int rightRecordCount) {
        return new MerkleRepairPlan(
                RANGE,
                List.of(new MerkleDifference(RANGE, leftRecordCount, rightRecordCount, new byte[] {1}, new byte[] {2}))
        );
    }

    private static ClusterNode node(String nodeId) {
        return new ClusterNode(new NodeId(nodeId), "127.0.0.1", 7000, Map.of());
    }

    private static Key keyInRange(TokenRange range, String prefix) {
        for (int index = 0; index < 10_000; index++) {
            Key key = Key.utf8(prefix + ":" + index);
            if (range.contains(KeyTokenHasher.tokenFor(key))) {
                return key;
            }
        }
        throw new IllegalStateException("unable to find key in range");
    }

    private static MutationRecord put(Key key, String value, int version) {
        return new MutationRecord(
                key,
                Optional.of(new Value(value.getBytes(StandardCharsets.UTF_8))),
                false,
                NOW.plusSeconds(version),
                Optional.empty(),
                "m" + version
        );
    }

    private static String value(StoredRecord record) {
        return new String(record.value().orElseThrow().bytes(), StandardCharsets.UTF_8);
    }

    private static final class InMemoryStorage implements StorageEngine {
        private final Map<Key, StoredRecord> records = new LinkedHashMap<>();

        @Override
        public void apply(MutationRecord mutation) {
            StoredRecord candidate = StoredRecord.from(mutation);
            StoredRecord existing = records.get(mutation.key());
            if (existing == null || candidate.compareVersionTo(existing) >= 0) {
                records.put(mutation.key(), candidate);
            }
        }

        @Override
        public Optional<StoredRecord> get(Key key) {
            return Optional.ofNullable(records.get(key));
        }

        @Override
        public List<StoredRecord> scanAll() {
            return new ArrayList<>(records.values());
        }

        @Override
        public byte[] digest(Key key) {
            return get(key)
                    .map(InMemoryStorage::recordDigest)
                    .orElseGet(() -> digest(new byte[0]));
        }

        @Override
        public void close() {
        }

        private static byte[] digest(byte[] bytes) {
            try {
                return MessageDigest.getInstance("SHA-256").digest(bytes);
            } catch (NoSuchAlgorithmException exception) {
                throw new IllegalStateException("SHA-256 is required by the JDK", exception);
            }
        }

        private static byte[] recordDigest(StoredRecord record) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                digest.update(record.mutationId().getBytes(StandardCharsets.UTF_8));
                digest.update((byte) (record.tombstone() ? 1 : 0));
                record.value().ifPresent(value -> digest.update(value.bytes()));
                return digest.digest();
            } catch (NoSuchAlgorithmException exception) {
                throw new IllegalStateException("SHA-256 is required by the JDK", exception);
            }
        }
    }
}
