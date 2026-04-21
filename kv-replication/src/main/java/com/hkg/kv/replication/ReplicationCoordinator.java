package com.hkg.kv.replication;

import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.storage.MutationRecord;
import java.util.ArrayList;
import java.util.List;

public final class ReplicationCoordinator {
    private final ReplicaWriter replicaWriter;

    public ReplicationCoordinator(ReplicaWriter replicaWriter) {
        if (replicaWriter == null) {
            throw new IllegalArgumentException("replica writer must not be null");
        }
        this.replicaWriter = replicaWriter;
    }

    public ReplicationResult replicate(ReplicationPlan plan, MutationRecord mutation) {
        return replicate(plan, mutation, null);
    }

    public ReplicationResult replicate(ReplicationPlan plan, MutationRecord mutation, Integer localReplicaCount) {
        if (plan == null) {
            throw new IllegalArgumentException("replication plan must not be null");
        }
        if (mutation == null) {
            throw new IllegalArgumentException("mutation record must not be null");
        }
        if (!plan.key().equals(mutation.key())) {
            throw new IllegalArgumentException("mutation key must match replication plan key");
        }

        ConsistencyWaitPolicy waitPolicy = ConsistencyWaitPolicy.forLevel(
                plan.consistencyLevel(),
                plan.replicas().size(),
                localReplicaCount
        );

        List<ReplicaResponse> responses = new ArrayList<>(plan.replicas().size());
        int successfulAcknowledgements = 0;
        for (ClusterNode replica : plan.replicas()) {
            ReplicaResponse response = invokeReplicaWriter(replica, mutation);
            responses.add(response);
            if (response.success()) {
                successfulAcknowledgements++;
            }
        }

        return new ReplicationResult(
                responses,
                successfulAcknowledgements,
                waitPolicy,
                successfulAcknowledgements >= waitPolicy.acknowledgementsRequired()
        );
    }

    private ReplicaResponse invokeReplicaWriter(ClusterNode replica, MutationRecord mutation) {
        try {
            ReplicaResponse response = replicaWriter.write(replica, mutation);
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
