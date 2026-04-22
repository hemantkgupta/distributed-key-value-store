package com.hkg.kv.repair;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.storage.StoredRecord;
import java.util.List;
import java.util.Optional;

public record ReadRepairPlan(Key key, Optional<StoredRecord> latestRecord, List<NodeId> targetNodeIds) {
    public ReadRepairPlan {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        latestRecord = latestRecord == null ? Optional.empty() : latestRecord;
        if (targetNodeIds == null) {
            throw new IllegalArgumentException("target node ids must not be null");
        }
        for (NodeId targetNodeId : targetNodeIds) {
            if (targetNodeId == null) {
                throw new IllegalArgumentException("target node ids must not contain null entries");
            }
        }
        targetNodeIds = List.copyOf(targetNodeIds);
    }

    public boolean requiresRepair() {
        return latestRecord.isPresent() && !targetNodeIds.isEmpty();
    }
}
