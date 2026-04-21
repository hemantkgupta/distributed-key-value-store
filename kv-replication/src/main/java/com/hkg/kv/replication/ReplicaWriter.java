package com.hkg.kv.replication;

import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.storage.MutationRecord;

@FunctionalInterface
public interface ReplicaWriter {
    ReplicaResponse write(ClusterNode replica, MutationRecord mutation);
}
