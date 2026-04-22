package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.replication.ReplicationPlan;
import com.hkg.kv.replication.ReplicationResult;
import com.hkg.kv.storage.MutationRecord;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class HintedHandoffPlanner {
    private final HintedHandoffService hintedHandoffService;

    public HintedHandoffPlanner(HintedHandoffService hintedHandoffService) {
        if (hintedHandoffService == null) {
            throw new IllegalArgumentException("hinted handoff service must not be null");
        }
        this.hintedHandoffService = hintedHandoffService;
    }

    public List<HintRecord> recordFailedReplicas(
            ReplicationPlan plan,
            MutationRecord mutation,
            ReplicationResult result
    ) {
        if (plan == null) {
            throw new IllegalArgumentException("replication plan must not be null");
        }
        if (mutation == null) {
            throw new IllegalArgumentException("mutation must not be null");
        }
        if (result == null) {
            throw new IllegalArgumentException("replication result must not be null");
        }
        if (!plan.key().equals(mutation.key())) {
            throw new IllegalArgumentException("mutation key must match replication plan key");
        }

        Set<NodeId> plannedNodes = new HashSet<>();
        plan.replicas().forEach(replica -> plannedNodes.add(replica.nodeId()));

        ArrayList<HintRecord> hints = new ArrayList<>();
        for (ReplicaResponse response : result.failedResponses()) {
            if (!plannedNodes.contains(response.nodeId())) {
                throw new IllegalArgumentException("failed response did not belong to replication plan");
            }
            hints.add(hintedHandoffService.recordHint(response.nodeId(), mutation));
        }
        return List.copyOf(hints);
    }
}
