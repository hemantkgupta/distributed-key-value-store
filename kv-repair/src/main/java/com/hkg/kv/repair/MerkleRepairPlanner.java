package com.hkg.kv.repair;

import java.util.ArrayList;
import java.util.Arrays;

public final class MerkleRepairPlanner {
    public MerkleRepairPlan plan(MerkleTree left, MerkleTree right) {
        if (left == null) {
            throw new IllegalArgumentException("left tree must not be null");
        }
        if (right == null) {
            throw new IllegalArgumentException("right tree must not be null");
        }
        if (!left.range().equals(right.range())) {
            throw new IllegalArgumentException("Merkle trees must cover the same token range");
        }
        if (left.maxDepth() != right.maxDepth()) {
            throw new IllegalArgumentException("Merkle trees must use the same max depth");
        }

        ArrayList<MerkleDifference> differences = new ArrayList<>();
        compare(left.root(), right.root(), differences);
        return new MerkleRepairPlan(left.range(), differences);
    }

    private static void compare(
            MerkleTreeNode left,
            MerkleTreeNode right,
            ArrayList<MerkleDifference> differences
    ) {
        if (Arrays.equals(left.hash(), right.hash())) {
            return;
        }

        if (!left.isLeaf() && !right.isLeaf()) {
            compare(left.left().orElseThrow(), right.left().orElseThrow(), differences);
            compare(left.right().orElseThrow(), right.right().orElseThrow(), differences);
            return;
        }

        differences.add(new MerkleDifference(
                left.range(),
                left.recordCount(),
                right.recordCount(),
                left.hash(),
                right.hash()
        ));
    }
}
