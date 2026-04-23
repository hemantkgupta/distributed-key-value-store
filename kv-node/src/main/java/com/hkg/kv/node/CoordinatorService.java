package com.hkg.kv.node;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.repair.ReadRepairExecutor;
import com.hkg.kv.repair.ReadRepairPlan;
import com.hkg.kv.repair.ReadRepairPlanner;
import com.hkg.kv.repair.ReadRepairResult;
import com.hkg.kv.replication.ConsistencyWaitPolicy;
import com.hkg.kv.replication.DigestReadCoordinator;
import com.hkg.kv.replication.DigestReadResult;
import com.hkg.kv.replication.ReplicaReadResponse;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.replication.ReplicaReader;
import com.hkg.kv.replication.ReplicaWriter;
import com.hkg.kv.replication.ReplicationCoordinator;
import com.hkg.kv.replication.ReplicationOptions;
import com.hkg.kv.replication.ReplicationPlan;
import com.hkg.kv.replication.ReplicationResult;
import com.hkg.kv.storage.StoredRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class CoordinatorService {
    private final List<ClusterNode> replicas;
    private final CoordinatorConfig config;
    private final ReplicationCoordinator replicationCoordinator;
    private final DigestReadCoordinator digestReadCoordinator;
    private final ReadRepairPlanner readRepairPlanner;
    private final ReadRepairExecutor readRepairExecutor;

    public CoordinatorService(
            List<ClusterNode> replicas,
            CoordinatorConfig config,
            ReplicaWriter replicaWriter,
            ReplicaReader replicaReader
    ) {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException("coordinator replicas must not be empty");
        }
        ArrayList<ClusterNode> resolvedReplicas = new ArrayList<>(replicas.size());
        for (ClusterNode replica : replicas) {
            if (replica == null) {
                throw new IllegalArgumentException("coordinator replicas must not contain null entries");
            }
            resolvedReplicas.add(replica);
        }
        if (config == null) {
            throw new IllegalArgumentException("coordinator config must not be null");
        }
        if (replicaWriter == null) {
            throw new IllegalArgumentException("replica writer must not be null");
        }
        if (replicaReader == null) {
            throw new IllegalArgumentException("replica reader must not be null");
        }
        this.replicas = List.copyOf(resolvedReplicas);
        this.config = config;
        this.replicationCoordinator = new ReplicationCoordinator(replicaWriter);
        this.digestReadCoordinator = new DigestReadCoordinator(replicaReader);
        this.readRepairPlanner = new ReadRepairPlanner();
        this.readRepairExecutor = new ReadRepairExecutor(replicaWriter);
    }

    public CoordinatorWriteResponse write(CoordinatorWriteRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("coordinator write request must not be null");
        }
        ReplicationResult result = replicationCoordinator.replicate(
                new ReplicationPlan(request.mutation().key(), replicas, request.consistencyLevel()),
                request.mutation(),
                replicationOptions(request.consistencyLevel())
        );
        return new CoordinatorWriteResponse(
                result.consistencySatisfied(),
                result.successfulAcknowledgements(),
                result.waitPolicy().acknowledgementsRequired(),
                result.totalAttempts(),
                replicaFailureDetails(result.failedResponses())
        );
    }

    public CoordinatorReadResponse read(CoordinatorReadRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("coordinator read request must not be null");
        }
        ReplicationPlan plan = new ReplicationPlan(request.key(), replicas, request.consistencyLevel());
        DigestReadResult readResult = digestReadCoordinator.read(plan);
        ConsistencyWaitPolicy waitPolicy = ConsistencyWaitPolicy.forLevel(
                request.consistencyLevel(),
                replicas.size(),
                resolveLocalReplicaCount(request.consistencyLevel())
        );
        int successfulResponses = countSuccessfulResponses(readResult, request.consistencyLevel());
        if (successfulResponses < waitPolicy.acknowledgementsRequired()) {
            return new CoordinatorReadResponse(
                    false,
                    Optional.empty(),
                    successfulResponses,
                    waitPolicy.acknowledgementsRequired(),
                    readResult.digestsAgree(),
                    0,
                    0,
                    readFailureDetails(readResult.failedResponses()),
                    "insufficient successful responses"
            );
        }

        ReadRepairResult repairResult = noRepair();
        if (config.readRepairEnabled() && !readResult.digestsAgree()) {
            ReadRepairPlan repairPlan = readRepairPlanner.plan(readResult);
            if (repairPlan.requiresRepair()) {
                repairResult = readRepairExecutor.execute(repairPlan, replicas);
            }
        }

        return new CoordinatorReadResponse(
                true,
                latestRecord(readResult),
                successfulResponses,
                waitPolicy.acknowledgementsRequired(),
                readResult.digestsAgree(),
                repairResult.successfulRepairs(),
                repairResult.failedRepairs(),
                readFailureDetails(readResult.failedResponses()),
                "ok"
        );
    }

    public List<ClusterNode> replicas() {
        return replicas;
    }

    private ReplicationOptions replicationOptions(ConsistencyLevel consistencyLevel) {
        ReplicationOptions options = ReplicationOptions.defaults().withMaxAttempts(config.maxAttempts());
        if (consistencyLevel == ConsistencyLevel.LOCAL_QUORUM && config.localDatacenter() != null) {
            options = options.withLocalDatacenter(config.localDatacenter());
        }
        return options;
    }

    private Integer resolveLocalReplicaCount(ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != ConsistencyLevel.LOCAL_QUORUM) {
            return null;
        }
        if (config.localDatacenter() == null) {
            throw new IllegalArgumentException("LOCAL_QUORUM requires kv.node.coordinator.local-datacenter");
        }
        int localReplicas = 0;
        for (ClusterNode replica : replicas) {
            String datacenter = replica.labels().get(ReplicationOptions.DATACENTER_LABEL);
            if (config.localDatacenter().equals(datacenter)) {
                localReplicas++;
            }
        }
        if (localReplicas < 1) {
            throw new IllegalArgumentException("configured local datacenter has no replicas");
        }
        return localReplicas;
    }

    private int countSuccessfulResponses(DigestReadResult readResult, ConsistencyLevel consistencyLevel) {
        if (consistencyLevel != ConsistencyLevel.LOCAL_QUORUM) {
            return readResult.successfulResponses().size();
        }
        int localSuccesses = 0;
        for (ReplicaReadResponse response : readResult.successfulResponses()) {
            ClusterNode replica = replicaByNodeId(response.nodeId().value());
            if (replica != null && config.localDatacenter().equals(replica.labels().get(ReplicationOptions.DATACENTER_LABEL))) {
                localSuccesses++;
            }
        }
        return localSuccesses;
    }

    private ClusterNode replicaByNodeId(String nodeId) {
        for (ClusterNode replica : replicas) {
            if (replica.nodeId().value().equals(nodeId)) {
                return replica;
            }
        }
        return null;
    }

    private static Optional<StoredRecord> latestRecord(DigestReadResult readResult) {
        return readResult.successfulResponses().stream()
                .flatMap(response -> response.record().stream())
                .max(StoredRecord::compareVersionTo);
    }

    private static ReadRepairResult noRepair() {
        return new ReadRepairResult(List.of(), 0);
    }

    private static List<String> replicaFailureDetails(List<ReplicaResponse> failedResponses) {
        ArrayList<String> details = new ArrayList<>(failedResponses.size());
        for (ReplicaResponse failedResponse : failedResponses) {
            details.add(failedResponse.nodeId().value() + ": " + failedResponse.detail());
        }
        return List.copyOf(details);
    }

    private static List<String> readFailureDetails(List<ReplicaReadResponse> failedResponses) {
        ArrayList<String> details = new ArrayList<>(failedResponses.size());
        for (ReplicaReadResponse failedResponse : failedResponses) {
            details.add(failedResponse.nodeId().value() + ": " + failedResponse.detail());
        }
        return List.copyOf(details);
    }
}
