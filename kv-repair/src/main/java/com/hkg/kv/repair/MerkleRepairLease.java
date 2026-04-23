package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import java.time.Instant;

public record MerkleRepairLease(
        String taskId,
        NodeId owner,
        long fencingToken,
        Instant acquiredAt,
        Instant expiresAt
) {
    public MerkleRepairLease {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("task id must not be blank");
        }
        if (owner == null) {
            throw new IllegalArgumentException("owner must not be null");
        }
        if (fencingToken < 1) {
            throw new IllegalArgumentException("fencing token must be positive");
        }
        if (acquiredAt == null) {
            throw new IllegalArgumentException("acquired time must not be null");
        }
        if (expiresAt == null || !expiresAt.isAfter(acquiredAt)) {
            throw new IllegalArgumentException("expiry time must be after acquired time");
        }
    }

    public boolean isExpiredAt(Instant now) {
        if (now == null) {
            throw new IllegalArgumentException("now must not be null");
        }
        return !now.isBefore(expiresAt);
    }
}
