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
