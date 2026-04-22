package com.hkg.kv.repair;

public record MerkleRepairTaskResult(
        MerkleRepairTask originalTask,
        MerkleRepairTask nextTask,
        MerkleRepairTaskStatus status,
        MerkleRepairResult repairResult,
        String failureMessage
) {
    public MerkleRepairTaskResult {
        if (originalTask == null) {
            throw new IllegalArgumentException("original task must not be null");
        }
        if (nextTask == null) {
            throw new IllegalArgumentException("next task must not be null");
        }
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        if (repairResult == null) {
            throw new IllegalArgumentException("repair result must not be null");
        }
        failureMessage = failureMessage == null ? "" : failureMessage;
    }

    public static MerkleRepairTaskResult notDue(MerkleRepairTask task) {
        return new MerkleRepairTaskResult(
                task,
                task,
                MerkleRepairTaskStatus.NOT_DUE,
                MerkleRepairResult.empty(),
                ""
        );
    }

    public static MerkleRepairTaskResult deferredByTaskBudget(MerkleRepairTask task) {
        return new MerkleRepairTaskResult(
                task,
                task,
                MerkleRepairTaskStatus.DEFERRED_BY_TASK_BUDGET,
                MerkleRepairResult.empty(),
                ""
        );
    }

    public static MerkleRepairTaskResult missingReplica(MerkleRepairTask task, MerkleRepairTask nextTask) {
        return new MerkleRepairTaskResult(
                task,
                nextTask,
                MerkleRepairTaskStatus.MISSING_REPLICA,
                MerkleRepairResult.empty(),
                "left or right replica storage is unavailable"
        );
    }

    public static MerkleRepairTaskResult noDifference(MerkleRepairTask task, MerkleRepairTask nextTask) {
        return new MerkleRepairTaskResult(
                task,
                nextTask,
                MerkleRepairTaskStatus.NO_DIFFERENCE,
                MerkleRepairResult.empty(),
                ""
        );
    }

    public static MerkleRepairTaskResult repaired(
            MerkleRepairTask task,
            MerkleRepairTask nextTask,
            MerkleRepairResult repairResult
    ) {
        return new MerkleRepairTaskResult(task, nextTask, MerkleRepairTaskStatus.REPAIRED, repairResult, "");
    }

    public static MerkleRepairTaskResult incomplete(
            MerkleRepairTask task,
            MerkleRepairTask nextTask,
            MerkleRepairResult repairResult
    ) {
        return new MerkleRepairTaskResult(task, nextTask, MerkleRepairTaskStatus.INCOMPLETE, repairResult, "");
    }

    public static MerkleRepairTaskResult failed(MerkleRepairTask task, MerkleRepairTask nextTask, String message) {
        return new MerkleRepairTaskResult(
                task,
                nextTask,
                MerkleRepairTaskStatus.FAILED,
                MerkleRepairResult.empty(),
                message == null || message.isBlank() ? "repair task failed" : message
        );
    }

    public boolean wasAttempted() {
        return status != MerkleRepairTaskStatus.NOT_DUE
                && status != MerkleRepairTaskStatus.DEFERRED_BY_TASK_BUDGET;
    }
}
