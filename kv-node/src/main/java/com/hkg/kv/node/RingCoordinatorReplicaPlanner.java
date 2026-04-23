package com.hkg.kv.node;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.partitioning.ConsistentHashReplicaPlacementPolicy;
import com.hkg.kv.replication.ReplicationPlan;
import java.util.Objects;

final class RingCoordinatorReplicaPlanner implements CoordinatorReplicaPlanner {
    private final ConsistentHashReplicaPlacementPolicy placementPolicy;
    private final int replicationFactor;

    RingCoordinatorReplicaPlanner(ConsistentHashReplicaPlacementPolicy placementPolicy, int replicationFactor) {
        this.placementPolicy = Objects.requireNonNull(placementPolicy, "placement policy must not be null");
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("ring replication factor must be positive");
        }
        this.replicationFactor = replicationFactor;
    }

    @Override
    public ReplicationPlan plan(Key key, ConsistencyLevel consistencyLevel) {
        return new ReplicationPlan(key, placementPolicy.replicasFor(key, replicationFactor), consistencyLevel);
    }

    @Override
    public java.util.List<ClusterNode> clusterNodes() {
        return placementPolicy.snapshot().nodes();
    }
}
