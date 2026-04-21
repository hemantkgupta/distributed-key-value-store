package com.hkg.kv.partitioning;

import com.hkg.kv.common.Key;
import java.util.List;
import java.util.Objects;

public final class ConsistentHashReplicaPlacementPolicy implements ReplicaPlacementPolicy {
    private final PartitionRingSnapshot snapshot;

    public ConsistentHashReplicaPlacementPolicy(PartitionRingSnapshot snapshot) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot must not be null");
    }

    public static ConsistentHashReplicaPlacementPolicy of(List<ClusterNode> nodes, int vnodeCountPerNode, long epoch) {
        return new ConsistentHashReplicaPlacementPolicy(PartitionRingSnapshot.of(nodes, vnodeCountPerNode, epoch));
    }

    public PartitionRingSnapshot snapshot() {
        return snapshot;
    }

    @Override
    public List<ClusterNode> replicasFor(Key key, int replicationFactor) {
        return snapshot.replicasFor(key, replicationFactor);
    }
}
