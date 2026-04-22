package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.storage.MutationRecord;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ConvergenceMetricsCollectorTest {
    @Test
    void reportsPendingHintBacklogShape() {
        InMemoryHintStore store = new InMemoryHintStore();
        store.append(hint("hint-1", Instant.parse("2026-01-01T00:00:00Z"), 1));
        store.append(hint("hint-2", Instant.parse("2026-01-01T00:00:05Z"), 3));

        ConvergenceMetricsSnapshot snapshot = new ConvergenceMetricsCollector(store).snapshot();

        assertThat(snapshot.pendingHints()).isEqualTo(2);
        assertThat(snapshot.oldestHintCreatedAt()).contains(Instant.parse("2026-01-01T00:00:00Z"));
        assertThat(snapshot.maxHintDeliveryAttempts()).isEqualTo(3);
    }

    @Test
    void includesReplayAndReadRepairOutcomeCounters() {
        InMemoryHintStore store = new InMemoryHintStore();
        ReadRepairResult readRepairResult = new ReadRepairResult(
                List.of(
                        new ReplicaResponse(new NodeId("node-1"), true, "ok"),
                        new ReplicaResponse(new NodeId("node-2"), false, "timeout")
                ),
                1
        );

        ConvergenceMetricsSnapshot snapshot = new ConvergenceMetricsCollector(store).snapshot(
                new HintReplaySummary(4, 3, 2, 1, 1),
                readRepairResult
        );

        assertThat(snapshot.attemptedHintReplays()).isEqualTo(3);
        assertThat(snapshot.deliveredHintReplays()).isEqualTo(2);
        assertThat(snapshot.failedHintReplays()).isEqualTo(1);
        assertThat(snapshot.skippedHintReplays()).isEqualTo(1);
        assertThat(snapshot.attemptedReadRepairs()).isEqualTo(2);
        assertThat(snapshot.successfulReadRepairs()).isEqualTo(1);
        assertThat(snapshot.failedReadRepairs()).isEqualTo(1);
        assertThat(snapshot.missingReadRepairTargets()).isEqualTo(1);
    }

    @Test
    void includesMerkleRepairOutcomeAndBudgetCounters() {
        InMemoryHintStore store = new InMemoryHintStore();

        ConvergenceMetricsSnapshot snapshot = new ConvergenceMetricsCollector(store).snapshot(
                null,
                null,
                new MerkleRepairResult(3, 7, 5, 2, 1, 1, 4, 1, true)
        );

        assertThat(snapshot.differingMerkleRepairRanges()).isEqualTo(3);
        assertThat(snapshot.scannedLeftMerkleRecords()).isEqualTo(7);
        assertThat(snapshot.scannedRightMerkleRecords()).isEqualTo(5);
        assertThat(snapshot.scannedMerkleRecords()).isEqualTo(12);
        assertThat(snapshot.merkleRepairWritesToLeft()).isEqualTo(2);
        assertThat(snapshot.merkleRepairWritesToRight()).isEqualTo(1);
        assertThat(snapshot.successfulMerkleRepairWrites()).isEqualTo(3);
        assertThat(snapshot.failedMerkleRepairWrites()).isEqualTo(1);
        assertThat(snapshot.alreadyConvergedMerkleKeys()).isEqualTo(4);
        assertThat(snapshot.skippedMerkleRepairRanges()).isEqualTo(1);
        assertThat(snapshot.merkleRepairBudgetStops()).isEqualTo(1);
    }

    @Test
    void exportsStableMetricSamples() {
        InMemoryHintStore store = new InMemoryHintStore();
        store.append(hint("hint-1", Instant.parse("2026-01-01T00:00:00Z"), 2));
        ConvergenceMetricsSnapshot snapshot = new ConvergenceMetricsCollector(store).snapshot(
                new HintReplaySummary(2, 2, 1, 1, 0),
                new ReadRepairResult(List.of(new ReplicaResponse(new NodeId("node-1"), true, "ok")), 0),
                new MerkleRepairResult(1, 2, 3, 1, 0, 0, 2, 0, false)
        );

        assertThat(snapshot.toMetrics())
                .contains(
                        new ConvergenceMetric("kv.repair.hints.pending", 1),
                        new ConvergenceMetric("kv.repair.hints.oldest_created_at_epoch_seconds", 1767225600L),
                        new ConvergenceMetric("kv.repair.hints.replay.attempted", 2),
                        new ConvergenceMetric("kv.repair.read_repair.successful", 1),
                        new ConvergenceMetric("kv.repair.merkle.ranges.differing", 1),
                        new ConvergenceMetric("kv.repair.merkle.records.scanned", 5),
                        new ConvergenceMetric("kv.repair.merkle.writes.successful", 1),
                        new ConvergenceMetric("kv.repair.merkle.budget_stops", 0)
                );
    }

    @Test
    void reporterExportsSnapshotMetricsToSink() {
        InMemoryHintStore store = new InMemoryHintStore();
        ConvergenceMetricsSnapshot snapshot = new ConvergenceMetricsCollector(store).snapshot();
        List<ConvergenceMetric> exported = new ArrayList<>();

        new ConvergenceMetricsReporter(exported::addAll).report(snapshot);

        assertThat(exported)
                .extracting(ConvergenceMetric::name)
                .contains("kv.repair.hints.pending", "kv.repair.merkle.budget_stops");
    }

    private static HintRecord hint(String hintId, Instant createdAt, int deliveryAttempts) {
        return new HintRecord(
                hintId,
                new NodeId("node-a"),
                new MutationRecord(
                        Key.utf8("user:1"),
                        Optional.of(new Value(new byte[] {1})),
                        false,
                        createdAt,
                        Optional.empty(),
                        "mutation-" + hintId
                ),
                createdAt,
                deliveryAttempts,
                Optional.empty()
        );
    }

    private static final class InMemoryHintStore implements HintStore {
        private final List<HintRecord> hints = new ArrayList<>();

        @Override
        public void append(HintRecord hint) {
            hints.add(hint);
        }

        @Override
        public List<HintRecord> loadAll() {
            return List.copyOf(hints);
        }

        @Override
        public void remove(String hintId) {
            hints.removeIf(hint -> hint.hintId().equals(hintId));
        }
    }
}
