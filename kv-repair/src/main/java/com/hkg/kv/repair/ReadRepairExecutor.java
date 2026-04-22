package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.replication.ReplicaWriter;
import com.hkg.kv.storage.MutationRecord;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ReadRepairExecutor {
    private final ReplicaWriter replicaWriter;

    public ReadRepairExecutor(ReplicaWriter replicaWriter) {
        if (replicaWriter == null) {
            throw new IllegalArgumentException("replica writer must not be null");
        }
        this.replicaWriter = replicaWriter;
    }

    public ReadRepairResult execute(ReadRepairPlan plan, List<ClusterNode> replicas) {
        if (plan == null) {
            throw new IllegalArgumentException("read repair plan must not be null");
        }
        if (replicas == null) {
            throw new IllegalArgumentException("replicas must not be null");
        }
        if (!plan.requiresRepair()) {
            return new ReadRepairResult(List.of(), 0);
        }

        Map<NodeId, ClusterNode> replicasByNodeId = new LinkedHashMap<>();
        for (ClusterNode replica : replicas) {
            if (replica == null) {
                throw new IllegalArgumentException("replicas must not contain null entries");
            }
            replicasByNodeId.put(replica.nodeId(), replica);
        }

        MutationRecord repairMutation = plan.latestRecord().orElseThrow().toMutationRecord();
        ArrayList<ReplicaResponse> responses = new ArrayList<>();
        int missingTargets = 0;
        for (NodeId targetNodeId : plan.targetNodeIds()) {
            ClusterNode replica = replicasByNodeId.get(targetNodeId);
            if (replica == null) {
                missingTargets++;
                continue;
            }
            responses.add(writeRepair(replica, repairMutation));
        }

        return new ReadRepairResult(responses, missingTargets);
    }

    private ReplicaResponse writeRepair(ClusterNode replica, MutationRecord repairMutation) {
        try {
            ReplicaResponse response = replicaWriter.write(replica, repairMutation);
            if (response == null) {
                return new ReplicaResponse(replica.nodeId(), false, "replica writer returned null response");
            }
            if (!replica.nodeId().equals(response.nodeId())) {
                return new ReplicaResponse(
                        replica.nodeId(),
                        false,
                        "replica writer response node id did not match targeted replica"
                );
            }
            return response;
        } catch (RuntimeException exception) {
            return new ReplicaResponse(replica.nodeId(), false, describeException(exception));
        }
    }

    private static String describeException(RuntimeException exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return exception.getClass().getSimpleName() + ": " + message;
    }
}
