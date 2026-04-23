package com.hkg.kv.node;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.replication.ReplicationPlan;
import java.util.ArrayList;
import java.util.List;

final class StaticCoordinatorReplicaPlanner implements CoordinatorReplicaPlanner {
    private final List<ClusterNode> replicas;

    StaticCoordinatorReplicaPlanner(List<ClusterNode> replicas) {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException("static coordinator replicas must not be empty");
        }
        ArrayList<ClusterNode> resolved = new ArrayList<>(replicas.size());
        for (ClusterNode replica : replicas) {
            if (replica == null) {
                throw new IllegalArgumentException("static coordinator replicas must not contain null entries");
            }
            resolved.add(replica);
        }
        this.replicas = List.copyOf(resolved);
    }

    @Override
    public ReplicationPlan plan(Key key, ConsistencyLevel consistencyLevel) {
        return new ReplicationPlan(key, replicas, consistencyLevel);
    }

    @Override
    public List<ClusterNode> clusterNodes() {
        return replicas;
    }
}
