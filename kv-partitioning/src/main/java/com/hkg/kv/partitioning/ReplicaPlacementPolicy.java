package com.hkg.kv.partitioning;

import com.hkg.kv.common.Key;
import java.util.List;

public interface ReplicaPlacementPolicy {
    List<ClusterNode> replicasFor(Key key, int replicationFactor);
}
