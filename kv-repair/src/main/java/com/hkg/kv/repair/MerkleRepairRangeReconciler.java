package com.hkg.kv.repair;

import com.hkg.kv.common.Key;
import com.hkg.kv.storage.StoredRecord;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class MerkleRepairRangeReconciler {
    private MerkleRepairRangeReconciler() {
    }

    static RepairCounters repairRange(
            List<StoredRecord> leftRecords,
            List<StoredRecord> rightRecords,
            MerkleRepairBudget budget,
            int writeAttemptsBeforeRange,
            RepairWriter repairWriter
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
        boolean stoppedByBudget = false;

        for (Key key : keys.keySet()) {
            Optional<StoredRecord> leftRecord = Optional.ofNullable(leftByKey.get(key));
            Optional<StoredRecord> rightRecord = Optional.ofNullable(rightByKey.get(key));

            int direction = repairDirection(leftRecord, rightRecord);
            if (direction == 0) {
                alreadyConvergedKeys++;
            } else if (direction < 0) {
                if (!budget.canWrite(writeAttempts(writeAttemptsBeforeRange, appliedToLeft, appliedToRight, failedWrites))) {
                    stoppedByBudget = true;
                    break;
                }
                if (repairWriter.applyToLeft(rightRecord.orElseThrow())) {
                    appliedToLeft++;
                } else {
                    failedWrites++;
                }
            } else {
                if (!budget.canWrite(writeAttempts(writeAttemptsBeforeRange, appliedToLeft, appliedToRight, failedWrites))) {
                    stoppedByBudget = true;
                    break;
                }
                if (repairWriter.applyToRight(leftRecord.orElseThrow())) {
                    appliedToRight++;
                } else {
                    failedWrites++;
                }
            }
        }

        return new RepairCounters(appliedToLeft, appliedToRight, failedWrites, alreadyConvergedKeys, stoppedByBudget);
    }

    static int writeAttempts(int appliedToLeft, int appliedToRight, int failedWrites) {
        return appliedToLeft + appliedToRight + failedWrites;
    }

    private static int writeAttempts(int beforeRange, int appliedToLeft, int appliedToRight, int failedWrites) {
        return beforeRange + appliedToLeft + appliedToRight + failedWrites;
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

    interface RepairWriter {
        boolean applyToLeft(StoredRecord record);

        boolean applyToRight(StoredRecord record);
    }

    record RepairCounters(
            int appliedToLeft,
            int appliedToRight,
            int failedWrites,
            int alreadyConvergedKeys,
            boolean stoppedByBudget
    ) {
    }
}
