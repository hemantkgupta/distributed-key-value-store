package com.hkg.kv.repair;

import com.hkg.kv.partitioning.TokenRange;
import java.util.List;

public record MerkleRepairPlan(TokenRange range, List<MerkleDifference> differences) {
    public MerkleRepairPlan {
        if (range == null) {
            throw new IllegalArgumentException("range must not be null");
        }
        if (differences == null) {
            throw new IllegalArgumentException("differences must not be null");
        }
        for (MerkleDifference difference : differences) {
            if (difference == null) {
                throw new IllegalArgumentException("differences must not contain null entries");
            }
        }
        differences = List.copyOf(differences);
    }

    public boolean requiresRepair() {
        return !differences.isEmpty();
    }
}
