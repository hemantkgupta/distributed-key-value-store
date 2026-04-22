package com.hkg.kv.repair;

import java.util.ArrayList;
import java.util.List;

public record MerkleRepairScheduleSummary(List<MerkleRepairTaskResult> taskResults) {
    public MerkleRepairScheduleSummary {
        if (taskResults == null) {
            throw new IllegalArgumentException("task results must not be null");
        }
        for (MerkleRepairTaskResult taskResult : taskResults) {
            if (taskResult == null) {
                throw new IllegalArgumentException("task results must not contain null entries");
            }
        }
        taskResults = List.copyOf(taskResults);
    }

    public List<MerkleRepairTask> nextTasks() {
        ArrayList<MerkleRepairTask> nextTasks = new ArrayList<>(taskResults.size());
        for (MerkleRepairTaskResult taskResult : taskResults) {
            nextTasks.add(taskResult.nextTask());
        }
        return List.copyOf(nextTasks);
    }

    public int attemptedTasks() {
        int attempted = 0;
        for (MerkleRepairTaskResult taskResult : taskResults) {
            if (taskResult.wasAttempted()) {
                attempted++;
            }
        }
        return attempted;
    }

    public int skippedNotDueTasks() {
        return count(MerkleRepairTaskStatus.NOT_DUE);
    }

    public int deferredByTaskBudgetTasks() {
        return count(MerkleRepairTaskStatus.DEFERRED_BY_TASK_BUDGET);
    }

    public int missingReplicaTasks() {
        return count(MerkleRepairTaskStatus.MISSING_REPLICA);
    }

    public int noDifferenceTasks() {
        return count(MerkleRepairTaskStatus.NO_DIFFERENCE);
    }

    public int repairedTasks() {
        return count(MerkleRepairTaskStatus.REPAIRED);
    }

    public int incompleteTasks() {
        return count(MerkleRepairTaskStatus.INCOMPLETE);
    }

    public int failedTasks() {
        return count(MerkleRepairTaskStatus.FAILED);
    }

    public int budgetStoppedTasks() {
        int stopped = 0;
        for (MerkleRepairTaskResult taskResult : taskResults) {
            if (taskResult.repairResult().stoppedByBudget()) {
                stopped++;
            }
        }
        return stopped;
    }

    public MerkleRepairResult aggregateRepairResult() {
        int differingRanges = 0;
        int scannedLeftRecords = 0;
        int scannedRightRecords = 0;
        int appliedToLeft = 0;
        int appliedToRight = 0;
        int failedWrites = 0;
        int alreadyConvergedKeys = 0;
        int skippedRanges = 0;
        boolean stoppedByBudget = false;

        for (MerkleRepairTaskResult taskResult : taskResults) {
            MerkleRepairResult repairResult = taskResult.repairResult();
            differingRanges += repairResult.differingRanges();
            scannedLeftRecords += repairResult.scannedLeftRecords();
            scannedRightRecords += repairResult.scannedRightRecords();
            appliedToLeft += repairResult.appliedToLeft();
            appliedToRight += repairResult.appliedToRight();
            failedWrites += repairResult.failedWrites();
            alreadyConvergedKeys += repairResult.alreadyConvergedKeys();
            skippedRanges += repairResult.skippedRanges();
            stoppedByBudget = stoppedByBudget || repairResult.stoppedByBudget();
        }

        return new MerkleRepairResult(
                differingRanges,
                scannedLeftRecords,
                scannedRightRecords,
                appliedToLeft,
                appliedToRight,
                failedWrites,
                alreadyConvergedKeys,
                skippedRanges,
                stoppedByBudget
        );
    }

    private int count(MerkleRepairTaskStatus status) {
        int matching = 0;
        for (MerkleRepairTaskResult taskResult : taskResults) {
            if (taskResult.status() == status) {
                matching++;
            }
        }
        return matching;
    }
}
