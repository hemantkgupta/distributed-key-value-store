package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.storage.StorageEngine;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class MerkleRepairScheduler {
    private final MerkleRangeScanner rangeScanner;
    private final MerkleRepairPlanner repairPlanner;
    private final MerkleRepairExecutor repairExecutor;
    private final MerkleRepairSchedulePolicy schedulePolicy;
    private final Clock clock;

    public MerkleRepairScheduler() {
        this(
                new MerkleRangeScanner(),
                new MerkleRepairPlanner(),
                new MerkleRepairExecutor(),
                MerkleRepairSchedulePolicy.defaults(),
                Clock.systemUTC()
        );
    }

    public MerkleRepairScheduler(MerkleRepairSchedulePolicy schedulePolicy, Clock clock) {
        this(
                new MerkleRangeScanner(),
                new MerkleRepairPlanner(),
                new MerkleRepairExecutor(),
                schedulePolicy,
                clock
        );
    }

    public MerkleRepairScheduler(
            MerkleRangeScanner rangeScanner,
            MerkleRepairPlanner repairPlanner,
            MerkleRepairExecutor repairExecutor,
            MerkleRepairSchedulePolicy schedulePolicy,
            Clock clock
    ) {
        if (rangeScanner == null) {
            throw new IllegalArgumentException("range scanner must not be null");
        }
        if (repairPlanner == null) {
            throw new IllegalArgumentException("repair planner must not be null");
        }
        if (repairExecutor == null) {
            throw new IllegalArgumentException("repair executor must not be null");
        }
        if (schedulePolicy == null) {
            throw new IllegalArgumentException("schedule policy must not be null");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        this.rangeScanner = rangeScanner;
        this.repairPlanner = repairPlanner;
        this.repairExecutor = repairExecutor;
        this.schedulePolicy = schedulePolicy;
        this.clock = clock;
    }

    public MerkleRepairScheduleSummary runDueTasks(
            List<MerkleRepairTask> tasks,
            Map<NodeId, StorageEngine> replicas,
            MerkleRepairScheduleBudget scheduleBudget
    ) {
        if (tasks == null) {
            throw new IllegalArgumentException("tasks must not be null");
        }
        if (replicas == null) {
            throw new IllegalArgumentException("replicas must not be null");
        }
        if (scheduleBudget == null) {
            throw new IllegalArgumentException("schedule budget must not be null");
        }

        Instant now = Instant.now(clock);
        ArrayList<MerkleRepairTaskResult> taskResults = new ArrayList<>();
        int attemptedTasks = 0;

        for (MerkleRepairTask task : tasks) {
            if (task == null) {
                throw new IllegalArgumentException("tasks must not contain null entries");
            }
            if (!task.isDue(now)) {
                taskResults.add(MerkleRepairTaskResult.notDue(task));
                continue;
            }
            if (attemptedTasks >= scheduleBudget.maxTasksPerRun()) {
                taskResults.add(MerkleRepairTaskResult.deferredByTaskBudget(task));
                continue;
            }

            attemptedTasks++;
            taskResults.add(runTask(task, replicas, scheduleBudget.repairBudgetPerTask(), now));
        }

        return new MerkleRepairScheduleSummary(taskResults);
    }

    private MerkleRepairTaskResult runTask(
            MerkleRepairTask task,
            Map<NodeId, StorageEngine> replicas,
            MerkleRepairBudget repairBudget,
            Instant now
    ) {
        StorageEngine left = replicas.get(task.leftReplica());
        StorageEngine right = replicas.get(task.rightReplica());
        if (left == null || right == null) {
            return MerkleRepairTaskResult.missingReplica(task, nextIncompleteTask(task, now));
        }

        try {
            MerkleTree leftTree = rangeScanner.treeFor(left, task.range(), task.maxDepth());
            MerkleTree rightTree = rangeScanner.treeFor(right, task.range(), task.maxDepth());
            MerkleRepairPlan repairPlan = repairPlanner.plan(leftTree, rightTree);
            if (!repairPlan.requiresRepair()) {
                return MerkleRepairTaskResult.noDifference(task, task.withCleanRun(schedulePolicy.nextCleanRunAt(now)));
            }

            MerkleRepairResult repairResult = repairExecutor.execute(repairPlan, left, right, repairBudget);
            if (repairResult.fullyApplied()) {
                return MerkleRepairTaskResult.repaired(
                        task,
                        task.withCleanRun(schedulePolicy.nextCleanRunAt(now)),
                        repairResult
                );
            }
            return MerkleRepairTaskResult.incomplete(task, nextIncompleteTask(task, now), repairResult);
        } catch (RuntimeException exception) {
            return MerkleRepairTaskResult.failed(task, nextIncompleteTask(task, now), exception.getMessage());
        }
    }

    private MerkleRepairTask nextIncompleteTask(MerkleRepairTask task, Instant now) {
        int nextIncompleteRuns = task.consecutiveIncompleteRuns() + 1;
        return task.withIncompleteRun(schedulePolicy.nextIncompleteRunAt(nextIncompleteRuns, now));
    }
}
