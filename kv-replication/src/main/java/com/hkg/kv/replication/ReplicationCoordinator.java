package com.hkg.kv.replication;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.storage.MutationRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ReplicationCoordinator {
    private static final String BUDGET_EXHAUSTED_BEFORE_CONTACT =
            "request budget exhausted before contacting replica";

    private final ReplicaWriter replicaWriter;

    public ReplicationCoordinator(ReplicaWriter replicaWriter) {
        if (replicaWriter == null) {
            throw new IllegalArgumentException("replica writer must not be null");
        }
        this.replicaWriter = replicaWriter;
    }

    public ReplicationResult replicate(ReplicationPlan plan, MutationRecord mutation) {
        return replicate(plan, mutation, ReplicationOptions.defaults());
    }

    public ReplicationResult replicate(ReplicationPlan plan, MutationRecord mutation, Integer localReplicaCount) {
        ReplicationOptions options = localReplicaCount == null
                ? ReplicationOptions.defaults()
                : ReplicationOptions.withLocalReplicaCount(localReplicaCount);
        return replicate(plan, mutation, options);
    }

    public ReplicationResult replicate(ReplicationPlan plan, MutationRecord mutation, ReplicationOptions options) {
        return replicate(plan, mutation, options, RequestBudget.unbounded());
    }

    public ReplicationResult replicate(
            ReplicationPlan plan,
            MutationRecord mutation,
            ReplicationOptions options,
            RequestBudget requestBudget
    ) {
        if (plan == null) {
            throw new IllegalArgumentException("replication plan must not be null");
        }
        if (mutation == null) {
            throw new IllegalArgumentException("mutation record must not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("replication options must not be null");
        }
        if (requestBudget == null) {
            throw new IllegalArgumentException("request budget must not be null");
        }
        if (!plan.key().equals(mutation.key())) {
            throw new IllegalArgumentException("mutation key must match replication plan key");
        }

        Integer resolvedLocalReplicaCount = resolveLocalReplicaCount(plan, options);
        ConsistencyWaitPolicy waitPolicy = ConsistencyWaitPolicy.forLevel(
                plan.consistencyLevel(),
                plan.replicas().size(),
                resolvedLocalReplicaCount
        );

        List<ReplicaResponse> responses = new ArrayList<>(plan.replicas().size());
        int successfulAcknowledgements = 0;
        for (ClusterNode replica : plan.replicas()) {
            ReplicaResponse response = invokeReplicaWriter(replica, mutation, options.maxAttempts(), requestBudget);
            responses.add(response);
            if (response.success() && countsTowardAcknowledgement(plan, options, replica)) {
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

    private ReplicaResponse invokeReplicaWriter(
            ClusterNode replica,
            MutationRecord mutation,
            int maxAttempts,
            RequestBudget requestBudget
    ) {
        ReplicaResponse lastResponse = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (requestBudget.exhausted()) {
                if (lastResponse == null) {
                    return new ReplicaResponse(replica.nodeId(), false, BUDGET_EXHAUSTED_BEFORE_CONTACT, 0);
                }
                return new ReplicaResponse(
                        replica.nodeId(),
                        false,
                        "request budget exhausted after " + lastResponse.attempts()
                                + " attempt(s); last failure: " + lastResponse.detail(),
                        lastResponse.attempts()
                );
            }
            try {
                ReplicaResponse response = replicaWriter.write(replica, mutation);
                if (response == null) {
                    lastResponse = new ReplicaResponse(
                            replica.nodeId(),
                            false,
                            "replica writer returned null response",
                            attempt
                    );
                } else if (!replica.nodeId().equals(response.nodeId())) {
                    lastResponse = new ReplicaResponse(
                            replica.nodeId(),
                            false,
                            "replica writer response node id did not match targeted replica",
                            attempt
                    );
                } else {
                    lastResponse = new ReplicaResponse(replica.nodeId(), response.success(), response.detail(), attempt);
                }
            } catch (RuntimeException exception) {
                lastResponse = new ReplicaResponse(replica.nodeId(), false, describeException(exception), attempt);
            }

            if (lastResponse.success()) {
                return lastResponse;
            }
        }
        return lastResponse;
    }

    private static String describeException(RuntimeException exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return exception.getClass().getSimpleName() + ": " + message;
    }

    private static Integer resolveLocalReplicaCount(ReplicationPlan plan, ReplicationOptions options) {
        if (options.localDatacenter() == null) {
            return options.localReplicaCount();
        }

        int localReplicas = 0;
        for (ClusterNode replica : plan.replicas()) {
            if (isInDatacenter(replica, options.localDatacenter())) {
                localReplicas++;
            }
        }
        if (localReplicas == 0) {
            throw new IllegalArgumentException("local datacenter has no replicas in plan");
        }
        if (options.localReplicaCount() != null && !Objects.equals(options.localReplicaCount(), localReplicas)) {
            throw new IllegalArgumentException("local replica count does not match local datacenter replicas");
        }
        return localReplicas;
    }

    private static boolean countsTowardAcknowledgement(
            ReplicationPlan plan,
            ReplicationOptions options,
            ClusterNode replica
    ) {
        if (plan.consistencyLevel() != ConsistencyLevel.LOCAL_QUORUM) {
            return true;
        }
        if (options.localDatacenter() == null) {
            return true;
        }
        return isInDatacenter(replica, options.localDatacenter());
    }

    private static boolean isInDatacenter(ClusterNode replica, String datacenter) {
        return datacenter.equals(replica.labels().get(ReplicationOptions.DATACENTER_LABEL))
                || datacenter.equals(replica.labels().get(ReplicationOptions.DC_LABEL));
    }
}
