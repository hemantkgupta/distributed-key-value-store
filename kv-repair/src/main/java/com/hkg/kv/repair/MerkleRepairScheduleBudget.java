package com.hkg.kv.repair;

public record MerkleRepairScheduleBudget(int maxTasksPerRun, MerkleRepairBudget repairBudgetPerTask) {
    public MerkleRepairScheduleBudget {
        if (maxTasksPerRun < 1) {
            throw new IllegalArgumentException("max tasks per run must be positive");
        }
        if (repairBudgetPerTask == null) {
            throw new IllegalArgumentException("repair budget per task must not be null");
        }
    }

    public static MerkleRepairScheduleBudget unbounded() {
        return new MerkleRepairScheduleBudget(Integer.MAX_VALUE, MerkleRepairBudget.unbounded());
    }
}
