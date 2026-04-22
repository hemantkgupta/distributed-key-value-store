package com.hkg.kv.repair;

public record MerkleRepairResult(
        int differingRanges,
        int scannedLeftRecords,
        int scannedRightRecords,
        int appliedToLeft,
        int appliedToRight,
        int failedWrites,
        int alreadyConvergedKeys
) {
    public MerkleRepairResult {
        if (differingRanges < 0
                || scannedLeftRecords < 0
                || scannedRightRecords < 0
                || appliedToLeft < 0
                || appliedToRight < 0
                || failedWrites < 0
                || alreadyConvergedKeys < 0) {
            throw new IllegalArgumentException("Merkle repair result counts must not be negative");
        }
    }

    public static MerkleRepairResult empty() {
        return new MerkleRepairResult(0, 0, 0, 0, 0, 0, 0);
    }

    public int successfulWrites() {
        return appliedToLeft + appliedToRight;
    }

    public boolean fullyApplied() {
        return failedWrites == 0;
    }
}
