package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.storage.StorageEngine;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class LeasedMerkleRepairScheduler {
    private final MerkleRepairScheduler delegate;
    private final MerkleRepairLeaseStore leaseStore;
    private final NodeId owner;
    private final Duration leaseTtl;
    private final Clock clock;

    public LeasedMerkleRepairScheduler(
            MerkleRepairScheduler delegate,
            MerkleRepairLeaseStore leaseStore,
            NodeId owner,
            Duration leaseTtl,
            Clock clock
    ) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate scheduler must not be null");
        }
        if (leaseStore == null) {
            throw new IllegalArgumentException("lease store must not be null");
        }
        if (owner == null) {
            throw new IllegalArgumentException("owner must not be null");
        }
        if (leaseTtl == null || leaseTtl.isZero() || leaseTtl.isNegative()) {
            throw new IllegalArgumentException("lease ttl must be positive");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        this.delegate = delegate;
        this.leaseStore = leaseStore;
        this.owner = owner;
        this.leaseTtl = leaseTtl;
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
        int claimedTasks = 0;

        for (MerkleRepairTask task : tasks) {
            if (task == null) {
                throw new IllegalArgumentException("tasks must not contain null entries");
            }
            if (!task.isDue(now)) {
                taskResults.add(MerkleRepairTaskResult.notDue(task));
                continue;
            }
            if (claimedTasks >= scheduleBudget.maxTasksPerRun()) {
                taskResults.add(MerkleRepairTaskResult.deferredByTaskBudget(task));
                continue;
            }

            Optional<MerkleRepairLease> lease = leaseStore.tryAcquire(task.taskId(), owner, now, leaseTtl);
            if (lease.isEmpty()) {
                taskResults.add(MerkleRepairTaskResult.leaseNotAcquired(task));
                continue;
            }

            claimedTasks++;
            taskResults.add(runClaimedTask(task, replicas, scheduleBudget.repairBudgetPerTask(), lease.orElseThrow()));
        }

        return new MerkleRepairScheduleSummary(taskResults);
    }

    private MerkleRepairTaskResult runClaimedTask(
            MerkleRepairTask task,
            Map<NodeId, StorageEngine> replicas,
            MerkleRepairBudget repairBudget,
            MerkleRepairLease lease
    ) {
        try {
            return delegate.runDueTasks(
                    List.of(task),
                    replicas,
                    new MerkleRepairScheduleBudget(1, repairBudget)
            ).taskResults().get(0);
        } finally {
            leaseStore.release(lease);
        }
    }
}
