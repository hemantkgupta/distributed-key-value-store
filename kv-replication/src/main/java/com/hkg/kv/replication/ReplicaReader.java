package com.hkg.kv.replication;

import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.ClusterNode;

@FunctionalInterface
public interface ReplicaReader {
    ReplicaReadResponse read(ClusterNode replica, Key key);
}
