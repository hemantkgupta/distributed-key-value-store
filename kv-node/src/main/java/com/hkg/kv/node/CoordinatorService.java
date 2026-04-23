package com.hkg.kv.node;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.repair.HintedHandoffPlanner;
import com.hkg.kv.repair.HintRecord;
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
import com.hkg.kv.replication.RequestBudget;
import com.hkg.kv.storage.StoredRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class CoordinatorService {
    private final CoordinatorReplicaPlanner replicaPlanner;
    private final CoordinatorConfig config;
    private final ReplicationCoordinator replicationCoordinator;
    private final DigestReadCoordinator digestReadCoordinator;
    private final ReadRepairPlanner readRepairPlanner;
    private final ReadRepairExecutor readRepairExecutor;
    private final HintedHandoffPlanner hintedHandoffPlanner;

    public CoordinatorService(
            CoordinatorReplicaPlanner replicaPlanner,
            CoordinatorConfig config,
            ReplicaWriter replicaWriter,
            ReplicaReader replicaReader,
            HintedHandoffPlanner hintedHandoffPlanner
    ) {
        if (replicaPlanner == null) {
            throw new IllegalArgumentException("coordinator replica planner must not be null");
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
        this.replicaPlanner = replicaPlanner;
        this.config = config;
        this.replicationCoordinator = new ReplicationCoordinator(replicaWriter);
        this.digestReadCoordinator = new DigestReadCoordinator(replicaReader);
        this.readRepairPlanner = new ReadRepairPlanner();
        this.readRepairExecutor = new ReadRepairExecutor(replicaWriter);
        this.hintedHandoffPlanner = hintedHandoffPlanner;
    }

    public CoordinatorWriteResponse write(CoordinatorWriteRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("coordinator write request must not be null");
        }
        RequestBudget requestBudget = requestBudget();
        ReplicationPlan plan = replicaPlanner.plan(request.mutation().key(), request.consistencyLevel());
        ReplicationResult result = replicationCoordinator.replicate(
                plan,
                request.mutation(),
                replicationOptions(plan),
                requestBudget
        );
        int durableHintsRecorded = recordFailedReplicas(plan, request, result).size();
        return new CoordinatorWriteResponse(
                result.consistencySatisfied() || consistencySatisfiedByHint(result.waitPolicy(), durableHintsRecorded),
                result.successfulAcknowledgements(),
                result.waitPolicy().acknowledgementsRequired(),
                durableHintsRecorded,
                result.totalAttempts(),
                replicaFailureDetails(result.failedResponses())
        );
    }

    public CoordinatorReadResponse read(CoordinatorReadRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("coordinator read request must not be null");
        }
        RequestBudget requestBudget = requestBudget();
        ReplicationPlan plan = replicaPlanner.plan(request.key(), request.consistencyLevel());
        DigestReadResult readResult = digestReadCoordinator.read(plan, requestBudget);
        ConsistencyWaitPolicy waitPolicy = ConsistencyWaitPolicy.forLevel(
                request.consistencyLevel(),
                plan.replicas().size(),
                resolveLocalReplicaCount(plan)
        );
        int successfulResponses = countSuccessfulResponses(readResult, plan);
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
        boolean readRepairSkippedByBudget = false;
        if (config.readRepairEnabled() && !readResult.digestsAgree() && requestBudget.exhausted()) {
            readRepairSkippedByBudget = true;
        } else if (config.readRepairEnabled() && !readResult.digestsAgree()) {
            ReadRepairPlan repairPlan = readRepairPlanner.plan(readResult);
            if (repairPlan.requiresRepair()) {
                repairResult = readRepairExecutor.execute(repairPlan, plan.replicas());
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
                readRepairSkippedByBudget ? "read repair skipped because request budget was exhausted" : "ok"
        );
    }

    public List<ClusterNode> replicas() {
        return replicaPlanner.clusterNodes();
    }

    public List<ClusterNode> clusterNodes() {
        return replicaPlanner.clusterNodes();
    }

    private ReplicationOptions replicationOptions(ReplicationPlan plan) {
        ReplicationOptions options = ReplicationOptions.defaults().withMaxAttempts(config.maxAttempts());
        if (plan.consistencyLevel() == ConsistencyLevel.LOCAL_QUORUM && config.localDatacenter() != null) {
            options = options.withLocalDatacenter(config.localDatacenter());
        }
        return options;
    }

    private Integer resolveLocalReplicaCount(ReplicationPlan plan) {
        if (plan.consistencyLevel() != ConsistencyLevel.LOCAL_QUORUM) {
            return null;
        }
        if (config.localDatacenter() == null) {
            throw new IllegalArgumentException("LOCAL_QUORUM requires kv.node.coordinator.local-datacenter");
        }
        int localReplicas = 0;
        for (ClusterNode replica : plan.replicas()) {
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

    private int countSuccessfulResponses(DigestReadResult readResult, ReplicationPlan plan) {
        if (plan.consistencyLevel() != ConsistencyLevel.LOCAL_QUORUM) {
            return readResult.successfulResponses().size();
        }
        int localSuccesses = 0;
        for (ReplicaReadResponse response : readResult.successfulResponses()) {
            ClusterNode replica = replicaByNodeId(plan.replicas(), response.nodeId().value());
            if (replica != null && config.localDatacenter().equals(replica.labels().get(ReplicationOptions.DATACENTER_LABEL))) {
                localSuccesses++;
            }
        }
        return localSuccesses;
    }

    private static ClusterNode replicaByNodeId(List<ClusterNode> replicas, String nodeId) {
        for (ClusterNode replica : replicas) {
            if (replica.nodeId().value().equals(nodeId)) {
                return replica;
            }
        }
        return null;
    }

    private List<HintRecord> recordFailedReplicas(
            ReplicationPlan plan,
            CoordinatorWriteRequest request,
            ReplicationResult result
    ) {
        if (!config.hintedHandoffEnabled() || hintedHandoffPlanner == null || result.failedResponses().isEmpty()) {
            return List.of();
        }
        return hintedHandoffPlanner.recordFailedReplicas(plan, request.mutation(), result);
    }

    private RequestBudget requestBudget() {
        return config.requestBudget() == null
                ? RequestBudget.unbounded()
                : RequestBudget.start(config.requestBudget());
    }

    private static boolean consistencySatisfiedByHint(ConsistencyWaitPolicy waitPolicy, int durableHintsRecorded) {
        return waitPolicy.hintMaySatisfyAcknowledgement() && durableHintsRecorded > 0;
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
