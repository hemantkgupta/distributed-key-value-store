package com.hkg.kv.repair;

import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.storage.StoredRecord;
import java.util.List;

@FunctionalInterface
public interface MerkleRangeStreamer {
    List<StoredRecord> stream(ClusterNode replica, TokenRange range);
}
