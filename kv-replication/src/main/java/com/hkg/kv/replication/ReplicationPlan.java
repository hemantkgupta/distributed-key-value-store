package com.hkg.kv.replication;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.ClusterNode;
import java.util.List;

public record ReplicationPlan(Key key, List<ClusterNode> replicas, ConsistencyLevel consistencyLevel) {
    public ReplicationPlan {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException("replicas must not be empty");
        }
        replicas = List.copyOf(replicas);
        if (consistencyLevel == null) {
            throw new IllegalArgumentException("consistency level must not be null");
        }
    }
}
