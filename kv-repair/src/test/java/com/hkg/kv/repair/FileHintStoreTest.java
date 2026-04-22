package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.storage.MutationRecord;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileHintStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void persistsHintsAcrossStoreInstances() {
        Path hintFile = tempDir.resolve("hints.log");
        FileHintStore store = new FileHintStore(hintFile);
        HintRecord hint = hint("hint-1", "node-a", mutation("mutation-1"));

        store.append(hint);

        FileHintStore reloadedStore = new FileHintStore(hintFile);
        assertThat(reloadedStore.loadAll())
                .singleElement()
                .satisfies(reloaded -> {
                    assertThat(reloaded.hintId()).isEqualTo("hint-1");
                    assertThat(reloaded.targetNodeId()).isEqualTo(new NodeId("node-a"));
                    assertThat(reloaded.createdAt()).isEqualTo(hint.createdAt());
                    assertThat(reloaded.deliveryAttempts()).isZero();
                    assertThat(reloaded.mutation().mutationId()).isEqualTo("mutation-1");
                    assertThat(reloaded.mutation().key()).isEqualTo(Key.utf8("user:1"));
                    assertThat(reloaded.mutation().value()).contains(new Value(new byte[] {1, 2, 3}));
                    assertThat(reloaded.mutation().ttl()).contains(Duration.ofSeconds(30));
                });
    }

    @Test
    void removesDeliveredHints() {
        Path hintFile = tempDir.resolve("hints.log");
        FileHintStore store = new FileHintStore(hintFile);
        store.append(hint("hint-1", "node-a", mutation("mutation-1")));
        store.append(hint("hint-2", "node-b", mutation("mutation-2")));

        store.remove("hint-1");

        assertThat(store.loadAll())
                .singleElement()
                .extracting(HintRecord::hintId)
                .isEqualTo("hint-2");
    }

    @Test
    void replacesFailedHintWithUpdatedAttemptMetadata() {
        Path hintFile = tempDir.resolve("hints.log");
        FileHintStore store = new FileHintStore(hintFile);
        HintRecord hint = hint("hint-1", "node-a", mutation("mutation-1"));

        store.append(hint);
        store.replace(hint.withFailedDelivery(Instant.parse("2026-01-01T00:01:00Z")));

        assertThat(store.loadAll())
                .singleElement()
                .satisfies(updated -> {
                    assertThat(updated.deliveryAttempts()).isEqualTo(1);
                    assertThat(updated.nextAttemptAt()).contains(Instant.parse("2026-01-01T00:01:00Z"));
                });
    }

    private static HintRecord hint(String hintId, String nodeId, MutationRecord mutation) {
        return new HintRecord(
                hintId,
                new NodeId(nodeId),
                mutation,
                Instant.parse("2026-01-01T00:00:00Z"),
                0,
                Optional.empty()
        );
    }

    private static MutationRecord mutation(String mutationId) {
        return new MutationRecord(
                Key.utf8("user:1"),
                Optional.of(new Value(new byte[] {1, 2, 3})),
                false,
                Instant.parse("2026-01-01T00:00:00Z"),
                Optional.of(Duration.ofSeconds(30)),
                mutationId
        );
    }
}
