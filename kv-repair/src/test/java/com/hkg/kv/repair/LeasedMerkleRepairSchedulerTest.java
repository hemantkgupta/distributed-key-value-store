package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StorageEngine;
import com.hkg.kv.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LeasedMerkleRepairSchedulerTest {
    private static final NodeId LEFT = new NodeId("node-left");
    private static final NodeId RIGHT = new NodeId("node-right");
    private static final NodeId WORKER_A = new NodeId("worker-a");
    private static final NodeId WORKER_B = new NodeId("worker-b");
    private static final TokenRange RANGE = TokenRange.fullRing();
    private static final Instant NOW = Instant.parse("2026-04-22T00:00:00Z");
    private static final Duration LEASE_TTL = Duration.ofSeconds(30);
    private static final MerkleRepairSchedulePolicy POLICY = new MerkleRepairSchedulePolicy(
            Duration.ofMinutes(5),
            Duration.ofSeconds(2),
            Duration.ofSeconds(8)
    );

    @Test
    void acquiresLeaseRunsTaskAndReleasesAfterCompletion() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(Key.utf8("user:1"), "left", 1));
        InMemoryMerkleRepairLeaseStore leaseStore = new InMemoryMerkleRepairLeaseStore();

        MerkleRepairScheduleSummary summary = leasedScheduler(leaseStore, WORKER_A).runDueTasks(
                List.of(task("repair-a", NOW)),
                Map.of(LEFT, left, RIGHT, right),
                MerkleRepairScheduleBudget.unbounded()
        );

        assertThat(summary.repairedTasks()).isEqualTo(1);
        assertThat(summary.leaseNotAcquiredTasks()).isZero();
        assertThat(leaseStore.currentLease("repair-a")).isEmpty();
        assertThat(right.get(Key.utf8("user:1"))).contains(left.get(Key.utf8("user:1")).orElseThrow());
    }

    @Test
    void skipsDueTaskWhenLeaseIsHeldByAnotherWorker() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        InMemoryMerkleRepairLeaseStore leaseStore = new InMemoryMerkleRepairLeaseStore();
        leaseStore.tryAcquire("repair-a", WORKER_B, NOW, LEASE_TTL);

        MerkleRepairScheduleSummary summary = leasedScheduler(leaseStore, WORKER_A).runDueTasks(
                List.of(task("repair-a", NOW)),
                Map.of(LEFT, left, RIGHT, right),
                MerkleRepairScheduleBudget.unbounded()
        );

        assertThat(summary.attemptedTasks()).isZero();
        assertThat(summary.leaseNotAcquiredTasks()).isEqualTo(1);
        assertThat(summary.taskResults().get(0).status()).isEqualTo(MerkleRepairTaskStatus.LEASE_NOT_ACQUIRED);
        assertThat(leaseStore.currentLease("repair-a").orElseThrow().owner()).isEqualTo(WORKER_B);
    }

    @Test
    void stealsExpiredLeaseAndRunsTask() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(Key.utf8("user:1"), "left", 1));
        InMemoryMerkleRepairLeaseStore leaseStore = new InMemoryMerkleRepairLeaseStore();
        leaseStore.tryAcquire("repair-a", WORKER_B, NOW.minusSeconds(60), Duration.ofSeconds(5));

        MerkleRepairScheduleSummary summary = leasedScheduler(leaseStore, WORKER_A).runDueTasks(
                List.of(task("repair-a", NOW)),
                Map.of(LEFT, left, RIGHT, right),
                MerkleRepairScheduleBudget.unbounded()
        );

        assertThat(summary.repairedTasks()).isEqualTo(1);
        assertThat(summary.leaseNotAcquiredTasks()).isZero();
        assertThat(leaseStore.currentLease("repair-a")).isEmpty();
        assertThat(right.get(Key.utf8("user:1"))).isPresent();
    }

    @Test
    void doesNotAcquireLeaseForTasksDeferredByTaskBudget() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        InMemoryMerkleRepairLeaseStore leaseStore = new InMemoryMerkleRepairLeaseStore();

        MerkleRepairScheduleSummary summary = leasedScheduler(leaseStore, WORKER_A).runDueTasks(
                List.of(task("repair-a", NOW), task("repair-b", NOW)),
                Map.of(LEFT, left, RIGHT, right),
                new MerkleRepairScheduleBudget(1, MerkleRepairBudget.unbounded())
        );

        assertThat(summary.noDifferenceTasks()).isEqualTo(1);
        assertThat(summary.deferredByTaskBudgetTasks()).isEqualTo(1);
        assertThat(leaseStore.currentLease("repair-a")).isEmpty();
        assertThat(leaseStore.currentLease("repair-b")).isEmpty();
    }

    private static LeasedMerkleRepairScheduler leasedScheduler(
            InMemoryMerkleRepairLeaseStore leaseStore,
            NodeId owner
    ) {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        MerkleRepairScheduler delegate = new MerkleRepairScheduler(POLICY, clock);
        return new LeasedMerkleRepairScheduler(delegate, leaseStore, owner, LEASE_TTL, clock);
    }

    private static MerkleRepairTask task(String taskId, Instant nextRunAt) {
        return new MerkleRepairTask(taskId, LEFT, RIGHT, RANGE, 4, nextRunAt, 0);
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
