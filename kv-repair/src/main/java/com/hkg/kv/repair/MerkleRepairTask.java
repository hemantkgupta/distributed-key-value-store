package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.partitioning.TokenRange;
import java.time.Instant;

public record MerkleRepairTask(
        String taskId,
        NodeId leftReplica,
        NodeId rightReplica,
        TokenRange range,
        int maxDepth,
        Instant nextRunAt,
        int consecutiveIncompleteRuns
) {
    public MerkleRepairTask {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("task id must not be blank");
        }
        if (leftReplica == null) {
            throw new IllegalArgumentException("left replica must not be null");
        }
        if (rightReplica == null) {
            throw new IllegalArgumentException("right replica must not be null");
        }
        if (leftReplica.equals(rightReplica)) {
            throw new IllegalArgumentException("repair task replicas must be distinct");
        }
        if (range == null) {
            throw new IllegalArgumentException("range must not be null");
        }
        if (maxDepth < 0 || maxDepth > 16) {
            throw new IllegalArgumentException("max depth must be between 0 and 16");
        }
        if (nextRunAt == null) {
            throw new IllegalArgumentException("next run time must not be null");
        }
        if (consecutiveIncompleteRuns < 0) {
            throw new IllegalArgumentException("consecutive incomplete runs must not be negative");
        }
    }

    public boolean isDue(Instant now) {
        if (now == null) {
            throw new IllegalArgumentException("now must not be null");
        }
        return !now.isBefore(nextRunAt);
    }

    public MerkleRepairTask withCleanRun(Instant nextRunAt) {
        return new MerkleRepairTask(taskId, leftReplica, rightReplica, range, maxDepth, nextRunAt, 0);
    }

    public MerkleRepairTask withIncompleteRun(Instant nextRunAt) {
        return new MerkleRepairTask(
                taskId,
                leftReplica,
                rightReplica,
                range,
                maxDepth,
                nextRunAt,
                consecutiveIncompleteRuns + 1
        );
    }
}
