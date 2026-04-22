package com.hkg.kv.repair;

public record MerkleRepairBudget(int maxRangesPerRun, int maxRecordsScannedPerRun, int maxWritesPerRun) {
    public MerkleRepairBudget {
        if (maxRangesPerRun < 1) {
            throw new IllegalArgumentException("max ranges per run must be positive");
        }
        if (maxRecordsScannedPerRun < 1) {
            throw new IllegalArgumentException("max records scanned per run must be positive");
        }
        if (maxWritesPerRun < 1) {
            throw new IllegalArgumentException("max writes per run must be positive");
        }
    }

    public static MerkleRepairBudget unbounded() {
        return new MerkleRepairBudget(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    boolean canStartRange(int processedRanges) {
        return processedRanges < maxRangesPerRun;
    }

    boolean canScan(int alreadyScannedRecords, int nextRangeRecords) {
        return (long) alreadyScannedRecords + nextRangeRecords <= maxRecordsScannedPerRun;
    }

    boolean canWrite(int attemptedWrites) {
        return attemptedWrites < maxWritesPerRun;
    }
}
