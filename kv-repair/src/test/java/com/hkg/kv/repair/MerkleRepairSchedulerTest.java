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

class MerkleRepairSchedulerTest {
    private static final NodeId LEFT = new NodeId("node-left");
    private static final NodeId RIGHT = new NodeId("node-right");
    private static final TokenRange RANGE = TokenRange.fullRing();
    private static final Instant NOW = Instant.parse("2026-04-22T00:00:00Z");
    private static final MerkleRepairSchedulePolicy POLICY = new MerkleRepairSchedulePolicy(
            Duration.ofMinutes(5),
            Duration.ofSeconds(2),
            Duration.ofSeconds(8)
    );

    @Test
    void repairsDueTaskAndReschedulesCleanRun() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(Key.utf8("user:1"), "left", 1));

        MerkleRepairScheduleSummary summary = scheduler().runDueTasks(
                List.of(task("repair-a", NOW.minusSeconds(1))),
                Map.of(LEFT, left, RIGHT, right),
                MerkleRepairScheduleBudget.unbounded()
        );

        assertThat(summary.attemptedTasks()).isEqualTo(1);
        assertThat(summary.repairedTasks()).isEqualTo(1);
        assertThat(summary.aggregateRepairResult().appliedToRight()).isEqualTo(1);
        assertThat(summary.taskResults().get(0).status()).isEqualTo(MerkleRepairTaskStatus.REPAIRED);
        assertThat(summary.nextTasks().get(0).nextRunAt()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
        assertThat(summary.nextTasks().get(0).consecutiveIncompleteRuns()).isZero();
        assertThat(right.get(Key.utf8("user:1"))).contains(left.get(Key.utf8("user:1")).orElseThrow());
    }

    @Test
    void skipsNotDueTasksAndDefersDueTasksAfterTaskBudget() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();

        MerkleRepairTask notDue = task("not-due", NOW.plusSeconds(10));
        MerkleRepairTask firstDue = task("first-due", NOW.minusSeconds(1));
        MerkleRepairTask secondDue = task("second-due", NOW.minusSeconds(1));
        MerkleRepairScheduleSummary summary = scheduler().runDueTasks(
                List.of(notDue, firstDue, secondDue),
                Map.of(LEFT, left, RIGHT, right),
                new MerkleRepairScheduleBudget(1, MerkleRepairBudget.unbounded())
        );

        assertThat(summary.attemptedTasks()).isEqualTo(1);
        assertThat(summary.skippedNotDueTasks()).isEqualTo(1);
        assertThat(summary.noDifferenceTasks()).isEqualTo(1);
        assertThat(summary.deferredByTaskBudgetTasks()).isEqualTo(1);
        assertThat(summary.nextTasks().get(0)).isEqualTo(notDue);
        assertThat(summary.nextTasks().get(2)).isEqualTo(secondDue);
    }

    @Test
    void reschedulesIncompleteTaskWhenRepairBudgetStopsRun() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(Key.utf8("user:1"), "first", 1));
        left.apply(put(Key.utf8("user:2"), "second", 2));

        MerkleRepairScheduleSummary summary = scheduler().runDueTasks(
                List.of(task("budgeted", NOW)),
                Map.of(LEFT, left, RIGHT, right),
                new MerkleRepairScheduleBudget(10, new MerkleRepairBudget(10, 10, 1))
        );

        assertThat(summary.incompleteTasks()).isEqualTo(1);
        assertThat(summary.budgetStoppedTasks()).isEqualTo(1);
        assertThat(summary.aggregateRepairResult().successfulWrites()).isEqualTo(1);
        assertThat(summary.nextTasks().get(0).nextRunAt()).isEqualTo(NOW.plusSeconds(2));
        assertThat(summary.nextTasks().get(0).consecutiveIncompleteRuns()).isEqualTo(1);
        assertThat(right.scanAll()).hasSize(1);
    }

    @Test
    void reschedulesMissingReplicaWithBackoff() {
        InMemoryStorage left = new InMemoryStorage();

        MerkleRepairScheduleSummary summary = scheduler().runDueTasks(
                List.of(task("missing", NOW)),
                Map.of(LEFT, left),
                MerkleRepairScheduleBudget.unbounded()
        );

        assertThat(summary.missingReplicaTasks()).isEqualTo(1);
        assertThat(summary.nextTasks().get(0).nextRunAt()).isEqualTo(NOW.plusSeconds(2));
        assertThat(summary.nextTasks().get(0).consecutiveIncompleteRuns()).isEqualTo(1);
        assertThat(summary.taskResults().get(0).failureMessage()).contains("unavailable");
    }

    @Test
    void capturesFailedTaskAndContinuesWithLaterTasks() {
        NodeId badLeft = new NodeId("bad-left");
        NodeId badRight = new NodeId("bad-right");
        NodeId goodLeft = new NodeId("good-left");
        NodeId goodRight = new NodeId("good-right");
        MerkleRepairTask badTask = new MerkleRepairTask("bad", badLeft, badRight, RANGE, 4, NOW, 0);
        MerkleRepairTask goodTask = new MerkleRepairTask("good", goodLeft, goodRight, RANGE, 4, NOW, 0);

        MerkleRepairScheduleSummary summary = scheduler().runDueTasks(
                List.of(badTask, goodTask),
                Map.of(
                        badLeft, new InMemoryStorage(true),
                        badRight, new InMemoryStorage(),
                        goodLeft, new InMemoryStorage(),
                        goodRight, new InMemoryStorage()
                ),
                MerkleRepairScheduleBudget.unbounded()
        );

        assertThat(summary.failedTasks()).isEqualTo(1);
        assertThat(summary.noDifferenceTasks()).isEqualTo(1);
        assertThat(summary.nextTasks().get(0).nextRunAt()).isEqualTo(NOW.plusSeconds(2));
        assertThat(summary.nextTasks().get(1).nextRunAt()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
    }

    private static MerkleRepairScheduler scheduler() {
        return new MerkleRepairScheduler(POLICY, Clock.fixed(NOW, ZoneOffset.UTC));
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
        private final boolean rejectScans;

        InMemoryStorage() {
            this(false);
        }

        InMemoryStorage(boolean rejectScans) {
            this.rejectScans = rejectScans;
        }

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
            if (rejectScans) {
                throw new IllegalStateException("scan rejected");
            }
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
