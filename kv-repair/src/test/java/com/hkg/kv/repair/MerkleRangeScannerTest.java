package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.TokenRange;
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

class MerkleRangeScannerTest {
    @Test
    void buildsTreeFromStorageRecordsInsideTokenRange() {
        InMemoryStorage storage = new InMemoryStorage();
        storage.apply(put("user:1", "a", 1));
        storage.apply(put("user:2", "b", 2));

        MerkleTree tree = new MerkleRangeScanner().treeFor(storage, TokenRange.fullRing(), 2);

        assertThat(tree.recordCount()).isEqualTo(2);
        assertThat(tree.rootHash()).hasSize(32);
    }

    @Test
    void returnsRecordsInDeterministicTokenOrder() {
        InMemoryStorage storage = new InMemoryStorage();
        storage.apply(put("user:2", "b", 2));
        storage.apply(put("user:1", "a", 1));

        List<StoredRecord> records = new MerkleRangeScanner().recordsInRange(storage, TokenRange.fullRing());

        assertThat(records).hasSize(2);
        assertThat(records).isSortedAccordingTo((left, right) ->
                com.hkg.kv.partitioning.KeyTokenHasher.tokenFor(left.key())
                        .compareTo(com.hkg.kv.partitioning.KeyTokenHasher.tokenFor(right.key())));
    }

    private static MutationRecord put(String key, String value, int version) {
        return new MutationRecord(
                Key.utf8(key),
                Optional.of(new Value(value.getBytes(StandardCharsets.UTF_8))),
                false,
                Instant.parse("2026-04-22T00:00:00Z").plusSeconds(version),
                Optional.empty(),
                "m" + version
        );
    }

    private static final class InMemoryStorage implements StorageEngine {
        private final Map<Key, StoredRecord> records = new LinkedHashMap<>();

        @Override
        public void apply(MutationRecord mutation) {
            records.put(mutation.key(), StoredRecord.from(mutation));
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
