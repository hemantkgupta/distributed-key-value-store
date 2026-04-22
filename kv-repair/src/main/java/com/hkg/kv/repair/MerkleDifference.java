package com.hkg.kv.repair;

import com.hkg.kv.partitioning.TokenRange;

public final class MerkleDifference {
    private final TokenRange range;
    private final int leftRecordCount;
    private final int rightRecordCount;
    private final byte[] leftHash;
    private final byte[] rightHash;

    public MerkleDifference(
            TokenRange range,
            int leftRecordCount,
            int rightRecordCount,
            byte[] leftHash,
            byte[] rightHash
    ) {
        if (range == null) {
            throw new IllegalArgumentException("range must not be null");
        }
        if (leftRecordCount < 0 || rightRecordCount < 0) {
            throw new IllegalArgumentException("record counts must not be negative");
        }
        if (leftHash == null || rightHash == null) {
            throw new IllegalArgumentException("hashes must not be null");
        }
        this.range = range;
        this.leftRecordCount = leftRecordCount;
        this.rightRecordCount = rightRecordCount;
        this.leftHash = leftHash.clone();
        this.rightHash = rightHash.clone();
    }

    public TokenRange range() {
        return range;
    }

    public int leftRecordCount() {
        return leftRecordCount;
    }

    public int rightRecordCount() {
        return rightRecordCount;
    }

    public byte[] leftHash() {
        return leftHash.clone();
    }

    public byte[] rightHash() {
        return rightHash.clone();
    }
}
