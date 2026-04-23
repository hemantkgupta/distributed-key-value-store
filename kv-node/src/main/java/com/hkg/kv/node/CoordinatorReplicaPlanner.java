package com.hkg.kv.node;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.replication.ReplicationPlan;
import java.util.List;

interface CoordinatorReplicaPlanner {
    ReplicationPlan plan(Key key, ConsistencyLevel consistencyLevel);

    List<ClusterNode> clusterNodes();
}
