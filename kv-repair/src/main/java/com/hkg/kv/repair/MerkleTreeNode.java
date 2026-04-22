package com.hkg.kv.repair;

import com.hkg.kv.partitioning.TokenRange;
import java.util.Optional;

public final class MerkleTreeNode {
    private final TokenRange range;
    private final byte[] hash;
    private final int recordCount;
    private final Optional<MerkleTreeNode> left;
    private final Optional<MerkleTreeNode> right;

    public MerkleTreeNode(
            TokenRange range,
            byte[] hash,
            int recordCount,
            Optional<MerkleTreeNode> left,
            Optional<MerkleTreeNode> right
    ) {
        if (range == null) {
            throw new IllegalArgumentException("range must not be null");
        }
        if (hash == null) {
            throw new IllegalArgumentException("hash must not be null");
        }
        if (recordCount < 0) {
            throw new IllegalArgumentException("record count must not be negative");
        }
        this.range = range;
        this.hash = hash.clone();
        this.recordCount = recordCount;
        this.left = left == null ? Optional.empty() : left;
        this.right = right == null ? Optional.empty() : right;
        if (this.left.isPresent() != this.right.isPresent()) {
            throw new IllegalArgumentException("Merkle node must have either zero or two children");
        }
    }

    public TokenRange range() {
        return range;
    }

    public byte[] hash() {
        return hash.clone();
    }

    public int recordCount() {
        return recordCount;
    }

    public Optional<MerkleTreeNode> left() {
        return left;
    }

    public Optional<MerkleTreeNode> right() {
        return right;
    }

    public boolean isLeaf() {
        return left.isEmpty();
    }
}
