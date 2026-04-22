package com.hkg.kv.repair;

import com.hkg.kv.common.Key;
import com.hkg.kv.storage.StorageEngine;
import com.hkg.kv.storage.StoredRecord;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class MerkleRepairExecutor {
    private final MerkleRangeScanner rangeScanner;

    public MerkleRepairExecutor(MerkleRangeScanner rangeScanner) {
        if (rangeScanner == null) {
            throw new IllegalArgumentException("range scanner must not be null");
        }
        this.rangeScanner = rangeScanner;
    }

    public MerkleRepairExecutor() {
        this(new MerkleRangeScanner());
    }

    public MerkleRepairResult execute(MerkleRepairPlan plan, StorageEngine left, StorageEngine right) {
        if (plan == null) {
            throw new IllegalArgumentException("Merkle repair plan must not be null");
        }
        if (left == null) {
            throw new IllegalArgumentException("left storage must not be null");
        }
        if (right == null) {
            throw new IllegalArgumentException("right storage must not be null");
        }
        if (!plan.requiresRepair()) {
            return MerkleRepairResult.empty();
        }

        int scannedLeftRecords = 0;
        int scannedRightRecords = 0;
        int appliedToLeft = 0;
        int appliedToRight = 0;
        int failedWrites = 0;
        int alreadyConvergedKeys = 0;

        for (MerkleDifference difference : plan.differences()) {
            List<StoredRecord> leftRecords = rangeScanner.recordsInRange(left, difference.range());
            List<StoredRecord> rightRecords = rangeScanner.recordsInRange(right, difference.range());
            scannedLeftRecords += leftRecords.size();
            scannedRightRecords += rightRecords.size();

            RepairCounters counters = repairRange(leftRecords, rightRecords, left, right);
            appliedToLeft += counters.appliedToLeft();
            appliedToRight += counters.appliedToRight();
            failedWrites += counters.failedWrites();
            alreadyConvergedKeys += counters.alreadyConvergedKeys();
        }

        return new MerkleRepairResult(
                plan.differences().size(),
                scannedLeftRecords,
                scannedRightRecords,
                appliedToLeft,
                appliedToRight,
                failedWrites,
                alreadyConvergedKeys
        );
    }

    private static RepairCounters repairRange(
            List<StoredRecord> leftRecords,
            List<StoredRecord> rightRecords,
            StorageEngine left,
            StorageEngine right
    ) {
        Map<Key, StoredRecord> leftByKey = byKey(leftRecords);
        Map<Key, StoredRecord> rightByKey = byKey(rightRecords);
        LinkedHashMap<Key, Boolean> keys = new LinkedHashMap<>();
        leftByKey.keySet().forEach(key -> keys.put(key, true));
        rightByKey.keySet().forEach(key -> keys.put(key, true));

        int appliedToLeft = 0;
        int appliedToRight = 0;
        int failedWrites = 0;
        int alreadyConvergedKeys = 0;

        for (Key key : keys.keySet()) {
            Optional<StoredRecord> leftRecord = Optional.ofNullable(leftByKey.get(key));
            Optional<StoredRecord> rightRecord = Optional.ofNullable(rightByKey.get(key));

            int direction = repairDirection(leftRecord, rightRecord);
            if (direction == 0) {
                alreadyConvergedKeys++;
            } else if (direction < 0) {
                if (apply(left, rightRecord.orElseThrow())) {
                    appliedToLeft++;
                } else {
                    failedWrites++;
                }
            } else {
                if (apply(right, leftRecord.orElseThrow())) {
                    appliedToRight++;
                } else {
                    failedWrites++;
                }
            }
        }

        return new RepairCounters(appliedToLeft, appliedToRight, failedWrites, alreadyConvergedKeys);
    }

    private static int repairDirection(Optional<StoredRecord> leftRecord, Optional<StoredRecord> rightRecord) {
        if (leftRecord.isPresent() && rightRecord.isPresent()) {
            int versionComparison = leftRecord.get().compareVersionTo(rightRecord.get());
            if (versionComparison != 0) {
                return Integer.compare(versionComparison, 0);
            }
            return leftRecord.get().equals(rightRecord.get()) ? 0 : 1;
        }
        if (leftRecord.isPresent()) {
            return 1;
        }
        if (rightRecord.isPresent()) {
            return -1;
        }
        return 0;
    }

    private static Map<Key, StoredRecord> byKey(List<StoredRecord> records) {
        LinkedHashMap<Key, StoredRecord> recordsByKey = new LinkedHashMap<>();
        for (StoredRecord record : records) {
            recordsByKey.put(record.key(), record);
        }
        return recordsByKey;
    }

    private static boolean apply(StorageEngine storage, StoredRecord record) {
        try {
            storage.apply(record.toMutationRecord());
            return true;
        } catch (RuntimeException exception) {
            return false;
        }
    }

    private record RepairCounters(
            int appliedToLeft,
            int appliedToRight,
            int failedWrites,
            int alreadyConvergedKeys
    ) {
    }
}
