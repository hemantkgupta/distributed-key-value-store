package com.hkg.kv.replication;

import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.ClusterNode;
import java.util.ArrayList;
import java.util.List;

public final class DigestReadCoordinator {
    private final ReplicaReader replicaReader;

    public DigestReadCoordinator(ReplicaReader replicaReader) {
        if (replicaReader == null) {
            throw new IllegalArgumentException("replica reader must not be null");
        }
        this.replicaReader = replicaReader;
    }

    public DigestReadResult read(ReplicationPlan plan) {
        if (plan == null) {
            throw new IllegalArgumentException("replication plan must not be null");
        }

        Key key = plan.key();
        List<ReplicaReadResponse> responses = new ArrayList<>(plan.replicas().size());
        for (ClusterNode replica : plan.replicas()) {
            responses.add(readReplica(replica, key));
        }
        return new DigestReadResult(key, responses);
    }

    private ReplicaReadResponse readReplica(ClusterNode replica, Key key) {
        try {
            ReplicaReadResponse response = replicaReader.read(replica, key);
            if (response == null) {
                return ReplicaReadResponse.failure(replica.nodeId(), "replica reader returned null response");
            }
            if (!replica.nodeId().equals(response.nodeId())) {
                return ReplicaReadResponse.failure(
                        replica.nodeId(),
                        "replica reader response node id did not match targeted replica"
                );
            }
            return response;
        } catch (RuntimeException exception) {
            return ReplicaReadResponse.failure(replica.nodeId(), describeException(exception));
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
