package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.storage.MutationRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class HintReplayWorkerTest {
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void removesDeliveredDueHints() {
        InMemoryHintStore store = new InMemoryHintStore();
        store.append(hint("hint-1", Optional.empty(), 0));
        HintReplayWorker worker = worker(store, hint -> true);

        HintReplaySummary summary = worker.replayDueHints();

        assertThat(summary).isEqualTo(new HintReplaySummary(1, 1, 1, 0, 0));
        assertThat(store.loadAll()).isEmpty();
    }

    @Test
    void updatesFailedHintWithBackoff() {
        InMemoryHintStore store = new InMemoryHintStore();
        store.append(hint("hint-1", Optional.empty(), 0));
        HintReplayWorker worker = worker(store, hint -> false);

        HintReplaySummary summary = worker.replayDueHints();

        assertThat(summary).isEqualTo(new HintReplaySummary(1, 1, 0, 1, 0));
        assertThat(store.loadAll())
                .singleElement()
                .satisfies(hint -> {
                    assertThat(hint.deliveryAttempts()).isEqualTo(1);
                    assertThat(hint.nextAttemptAt()).contains(NOW.plusSeconds(1));
                });
    }

    @Test
    void skipsHintsThatAreNotDueYet() {
        InMemoryHintStore store = new InMemoryHintStore();
        store.append(hint("hint-1", Optional.of(NOW.plusSeconds(5)), 1));
        HintReplayWorker worker = worker(store, hint -> true);

        HintReplaySummary summary = worker.replayDueHints();

        assertThat(summary).isEqualTo(new HintReplaySummary(1, 0, 0, 0, 1));
        assertThat(store.loadAll()).hasSize(1);
    }

    @Test
    void treatsDeliveryExceptionsAsFailures() {
        InMemoryHintStore store = new InMemoryHintStore();
        store.append(hint("hint-1", Optional.empty(), 1));
        HintReplayWorker worker = worker(store, hint -> {
            throw new IllegalStateException("node still down");
        });

        worker.replayDueHints();

        assertThat(store.loadAll())
                .singleElement()
                .satisfies(hint -> {
                    assertThat(hint.deliveryAttempts()).isEqualTo(2);
                    assertThat(hint.nextAttemptAt()).contains(NOW.plusSeconds(2));
                });
    }

    private static HintReplayWorker worker(InMemoryHintStore store, HintDelivery delivery) {
        return new HintReplayWorker(
                store,
                delivery,
                new HintReplayPolicy(Duration.ofSeconds(1), Duration.ofSeconds(30)),
                Clock.fixed(NOW, ZoneOffset.UTC)
        );
    }

    private static HintRecord hint(String hintId, Optional<Instant> nextAttemptAt, int deliveryAttempts) {
        return new HintRecord(
                hintId,
                new NodeId("node-a"),
                new MutationRecord(
                        Key.utf8("user:1"),
                        Optional.of(new Value(new byte[] {1})),
                        false,
                        NOW,
                        Optional.empty(),
                        "mutation-1"
                ),
                NOW,
                deliveryAttempts,
                nextAttemptAt
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
