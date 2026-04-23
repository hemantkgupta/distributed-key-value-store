package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class InMemoryMerkleRepairLeaseStore implements MerkleRepairLeaseStore {
    private final Map<String, MerkleRepairLease> leases = new HashMap<>();
    private long nextFencingToken = 1L;

    @Override
    public synchronized Optional<MerkleRepairLease> tryAcquire(
            String taskId,
            NodeId owner,
            Instant now,
            Duration leaseTtl
    ) {
        validate(taskId, owner, now, leaseTtl);

        MerkleRepairLease current = leases.get(taskId);
        if (current != null && !current.isExpiredAt(now)) {
            return Optional.empty();
        }

        MerkleRepairLease acquired = new MerkleRepairLease(
                taskId,
                owner,
                nextFencingToken++,
                now,
                now.plus(leaseTtl)
        );
        leases.put(taskId, acquired);
        return Optional.of(acquired);
    }

    @Override
    public synchronized boolean release(MerkleRepairLease lease) {
        if (lease == null) {
            throw new IllegalArgumentException("lease must not be null");
        }
        MerkleRepairLease current = leases.get(lease.taskId());
        if (lease.equals(current)) {
            leases.remove(lease.taskId());
            return true;
        }
        return false;
    }

    @Override
    public synchronized Optional<MerkleRepairLease> currentLease(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("task id must not be blank");
        }
        return Optional.ofNullable(leases.get(taskId));
    }

    private static void validate(String taskId, NodeId owner, Instant now, Duration leaseTtl) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("task id must not be blank");
        }
        if (owner == null) {
            throw new IllegalArgumentException("owner must not be null");
        }
        if (now == null) {
            throw new IllegalArgumentException("now must not be null");
        }
        if (leaseTtl == null || leaseTtl.isZero() || leaseTtl.isNegative()) {
            throw new IllegalArgumentException("lease ttl must be positive");
        }
    }
}
