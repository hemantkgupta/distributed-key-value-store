package com.hkg.kv.repair;

public enum MerkleRepairTaskStatus {
    NOT_DUE,
    DEFERRED_BY_TASK_BUDGET,
    LEASE_NOT_ACQUIRED,
    MISSING_REPLICA,
    NO_DIFFERENCE,
    REPAIRED,
    INCOMPLETE,
    FAILED
}
