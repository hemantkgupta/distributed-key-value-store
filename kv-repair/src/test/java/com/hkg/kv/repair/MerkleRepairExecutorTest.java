package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.KeyTokenHasher;
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

class MerkleRepairExecutorTest {
    private static final TokenRange RANGE = TokenRange.fullRing();

    @Test
    void appliesMissingRecordToOtherReplica() {
        Key key = Key.utf8("user:1");
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(key, "left", 1));

        MerkleRepairResult result = new MerkleRepairExecutor().execute(plan(left, right), left, right);

        assertThat(result.successfulWrites()).isEqualTo(1);
        assertThat(result.appliedToRight()).isEqualTo(1);
        assertThat(result.appliedToLeft()).isZero();
        assertThat(result.fullyApplied()).isTrue();
        assertThat(right.get(key)).contains(left.get(key).orElseThrow());
    }

    @Test
    void appliesNewestVersionToStaleReplica() {
        Key key = Key.utf8("user:1");
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(key, "old", 1));
        right.apply(put(key, "new", 2));

        MerkleRepairResult result = new MerkleRepairExecutor().execute(plan(left, right), left, right);

        assertThat(result.appliedToLeft()).isEqualTo(1);
        assertThat(result.appliedToRight()).isZero();
        assertThat(value(left.get(key).orElseThrow())).isEqualTo("new");
    }

    @Test
    void appliesTombstoneAsLatestVersion() {
        Key key = Key.utf8("user:1");
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(key, "live", 1));
        right.apply(delete(key, 2));

        MerkleRepairResult result = new MerkleRepairExecutor().execute(plan(left, right), left, right);

        assertThat(result.appliedToLeft()).isEqualTo(1);
        assertThat(left.get(key).orElseThrow().tombstone()).isTrue();
    }

    @Test
    void resolvesEqualVersionDivergenceDeterministicallyFromLeft() {
        Key key = Key.utf8("user:1");
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(key, "left-value", 1));
        right.apply(put(key, "right-value", 1));

        MerkleRepairResult result = new MerkleRepairExecutor().execute(plan(left, right), left, right);

        assertThat(result.appliedToRight()).isEqualTo(1);
        assertThat(value(right.get(key).orElseThrow())).isEqualTo("left-value");
    }

    @Test
    void emptyPlanDoesNotScanOrWrite() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();

        MerkleRepairResult result = new MerkleRepairExecutor().execute(
                new MerkleRepairPlan(RANGE, List.of()),
                left,
                right
        );

        assertThat(result).isEqualTo(MerkleRepairResult.empty());
    }

    @Test
    void capturesFailedRepairWrite() {
        Key key = Key.utf8("user:1");
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage(true);
        left.apply(put(key, "left", 1));

        MerkleRepairResult result = new MerkleRepairExecutor().execute(plan(left, right), left, right);

        assertThat(result.successfulWrites()).isZero();
        assertThat(result.failedWrites()).isEqualTo(1);
        assertThat(result.fullyApplied()).isFalse();
    }

    @Test
    void stopsAfterRangeBudgetIsExhausted() {
        List<TokenRange> ranges = RANGE.split();
        Key firstKey = keyInRange(ranges.get(0), "range-a");
        Key secondKey = keyInRange(ranges.get(1), "range-b");
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(firstKey, "first", 1));
        left.apply(put(secondKey, "second", 2));

        MerkleRepairResult result = new MerkleRepairExecutor().execute(
                manualPlan(List.of(
                        difference(ranges.get(0), 1, 0),
                        difference(ranges.get(1), 1, 0)
                )),
                left,
                right,
                new MerkleRepairBudget(1, 10, 10)
        );

        assertThat(result.appliedToRight()).isEqualTo(1);
        assertThat(result.skippedRanges()).isEqualTo(1);
        assertThat(result.stoppedByBudget()).isTrue();
        assertThat(right.get(firstKey)).isPresent();
        assertThat(right.get(secondKey)).isEmpty();
    }

    @Test
    void stopsBeforeScanningRangeWhenRecordBudgetWouldBeExceeded() {
        Key firstKey = Key.utf8("user:1");
        Key secondKey = Key.utf8("user:2");
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(firstKey, "first", 1));
        left.apply(put(secondKey, "second", 2));

        MerkleRepairResult result = new MerkleRepairExecutor().execute(
                manualPlan(List.of(difference(RANGE, 2, 0))),
                left,
                right,
                new MerkleRepairBudget(10, 1, 10)
        );

        assertThat(result.scannedLeftRecords()).isZero();
        assertThat(result.appliedToRight()).isZero();
        assertThat(result.skippedRanges()).isEqualTo(1);
        assertThat(result.stoppedByBudget()).isTrue();
    }

    @Test
    void stopsWithinRangeWhenWriteBudgetIsExhausted() {
        InMemoryStorage left = new InMemoryStorage();
        InMemoryStorage right = new InMemoryStorage();
        left.apply(put(Key.utf8("user:1"), "first", 1));
        left.apply(put(Key.utf8("user:2"), "second", 2));

        MerkleRepairResult result = new MerkleRepairExecutor().execute(
                manualPlan(List.of(difference(RANGE, 2, 0))),
                left,
                right,
                new MerkleRepairBudget(10, 10, 1)
        );

        assertThat(result.successfulWrites()).isEqualTo(1);
        assertThat(result.skippedRanges()).isEqualTo(1);
        assertThat(result.stoppedByBudget()).isTrue();
        assertThat(result.fullyApplied()).isFalse();
        assertThat(right.scanAll()).hasSize(1);
    }

    private static MerkleRepairPlan plan(StorageEngine left, StorageEngine right) {
        MerkleRangeScanner scanner = new MerkleRangeScanner();
        MerkleTree leftTree = scanner.treeFor(left, RANGE, 4);
        MerkleTree rightTree = scanner.treeFor(right, RANGE, 4);
        return new MerkleRepairPlanner().plan(leftTree, rightTree);
    }

    private static MerkleRepairPlan manualPlan(List<MerkleDifference> differences) {
        return new MerkleRepairPlan(RANGE, differences);
    }

    private static MerkleDifference difference(TokenRange range, int leftRecordCount, int rightRecordCount) {
        return new MerkleDifference(range, leftRecordCount, rightRecordCount, new byte[] {1}, new byte[] {2});
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
                Instant.parse("2026-04-22T00:00:00Z").plusSeconds(version),
                Optional.empty(),
                "m" + version
        );
    }

    private static MutationRecord delete(Key key, int version) {
        return new MutationRecord(
                key,
                Optional.empty(),
                true,
                Instant.parse("2026-04-22T00:00:00Z").plusSeconds(version),
                Optional.empty(),
                "m" + version
        );
    }

    private static String value(StoredRecord record) {
        return new String(record.value().orElseThrow().bytes(), StandardCharsets.UTF_8);
    }

    private static final class InMemoryStorage implements StorageEngine {
        private final Map<Key, StoredRecord> records = new LinkedHashMap<>();
        private final boolean rejectWrites;

        InMemoryStorage() {
            this(false);
        }

        InMemoryStorage(boolean rejectWrites) {
            this.rejectWrites = rejectWrites;
        }

        @Override
        public void apply(MutationRecord mutation) {
            if (rejectWrites) {
                throw new IllegalStateException("write rejected");
            }
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
