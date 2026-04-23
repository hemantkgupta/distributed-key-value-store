package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public interface MerkleRepairLeaseStore {
    Optional<MerkleRepairLease> tryAcquire(String taskId, NodeId owner, Instant now, Duration leaseTtl);

    boolean release(MerkleRepairLease lease);

    Optional<MerkleRepairLease> currentLease(String taskId);
}
