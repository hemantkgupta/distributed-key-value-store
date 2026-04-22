package com.hkg.kv.repair;

public record MerkleRepairResult(
        int differingRanges,
        int scannedLeftRecords,
        int scannedRightRecords,
        int appliedToLeft,
        int appliedToRight,
        int failedWrites,
        int alreadyConvergedKeys,
        int skippedRanges,
        boolean stoppedByBudget
) {
    public MerkleRepairResult(
            int differingRanges,
            int scannedLeftRecords,
            int scannedRightRecords,
            int appliedToLeft,
            int appliedToRight,
            int failedWrites,
            int alreadyConvergedKeys
    ) {
        this(
                differingRanges,
                scannedLeftRecords,
                scannedRightRecords,
                appliedToLeft,
                appliedToRight,
                failedWrites,
                alreadyConvergedKeys,
                0,
                false
        );
    }

    public MerkleRepairResult {
        if (differingRanges < 0
                || scannedLeftRecords < 0
                || scannedRightRecords < 0
                || appliedToLeft < 0
                || appliedToRight < 0
                || failedWrites < 0
                || alreadyConvergedKeys < 0
                || skippedRanges < 0) {
            throw new IllegalArgumentException("Merkle repair result counts must not be negative");
        }
        if (skippedRanges > differingRanges) {
            throw new IllegalArgumentException("skipped ranges must not exceed differing ranges");
        }
    }

    public static MerkleRepairResult empty() {
        return new MerkleRepairResult(0, 0, 0, 0, 0, 0, 0, 0, false);
    }

    public int successfulWrites() {
        return appliedToLeft + appliedToRight;
    }

    public boolean fullyApplied() {
        return failedWrites == 0 && skippedRanges == 0 && !stoppedByBudget;
    }
}
