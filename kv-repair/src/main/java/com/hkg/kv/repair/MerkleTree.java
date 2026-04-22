package com.hkg.kv.repair;

import com.hkg.kv.partitioning.TokenRange;

public final class MerkleTree {
    private final TokenRange range;
    private final MerkleTreeNode root;
    private final int maxDepth;

    public MerkleTree(TokenRange range, MerkleTreeNode root, int maxDepth) {
        if (range == null) {
            throw new IllegalArgumentException("range must not be null");
        }
        if (root == null) {
            throw new IllegalArgumentException("root must not be null");
        }
        if (maxDepth < 0) {
            throw new IllegalArgumentException("max depth must not be negative");
        }
        if (!range.equals(root.range())) {
            throw new IllegalArgumentException("root range must match tree range");
        }
        this.range = range;
        this.root = root;
        this.maxDepth = maxDepth;
    }

    public TokenRange range() {
        return range;
    }

    public MerkleTreeNode root() {
        return root;
    }

    public int maxDepth() {
        return maxDepth;
    }

    public int recordCount() {
        return root.recordCount();
    }

    public byte[] rootHash() {
        return root.hash();
    }
}
