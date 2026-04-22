package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StoredRecord;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ReadRepairExecutorTest {
    @Test
    void writesLatestRecordToTargetReplicas() {
        StoredRecord latest = record(2);
        ReadRepairPlan plan = new ReadRepairPlan(
                latest.key(),
                Optional.of(latest),
                List.of(new NodeId("node-1"), new NodeId("node-2"))
        );
        List<MutationRecord> mutations = new ArrayList<>();
        ReadRepairExecutor executor = new ReadRepairExecutor((replica, mutation) -> {
            mutations.add(mutation);
            return new ReplicaResponse(replica.nodeId(), true, "repaired");
        });

        ReadRepairResult result = executor.execute(plan, replicas(3));

        assertThat(result.successfulRepairs()).isEqualTo(2);
        assertThat(result.failedRepairs()).isZero();
        assertThat(result.missingTargets()).isZero();
        assertThat(mutations).hasSize(2);
        assertThat(mutations).allSatisfy(mutation -> {
            assertThat(mutation.key()).isEqualTo(latest.key());
            assertThat(mutation.timestamp()).isEqualTo(latest.timestamp());
            assertThat(mutation.mutationId()).isEqualTo(latest.mutationId());
        });
    }

    @Test
    void returnsEmptyResultWhenPlanDoesNotRequireRepair() {
        ReadRepairExecutor executor = new ReadRepairExecutor((replica, mutation) ->
                new ReplicaResponse(replica.nodeId(), true, "unexpected"));

        ReadRepairResult result = executor.execute(
                new ReadRepairPlan(Key.utf8("user:1"), Optional.empty(), List.of()),
                replicas(3)
        );

        assertThat(result.attemptedRepairs()).isZero();
        assertThat(result.missingTargets()).isZero();
    }

    @Test
    void countsMissingTargetReplicasWithoutFailingWholeRepair() {
        StoredRecord latest = record(2);
        ReadRepairPlan plan = new ReadRepairPlan(
                latest.key(),
                Optional.of(latest),
                List.of(new NodeId("node-1"), new NodeId("missing-node"))
        );
        ReadRepairExecutor executor = new ReadRepairExecutor((replica, mutation) ->
                new ReplicaResponse(replica.nodeId(), true, "repaired"));

        ReadRepairResult result = executor.execute(plan, replicas(2));

        assertThat(result.successfulRepairs()).isEqualTo(1);
        assertThat(result.missingTargets()).isEqualTo(1);
    }

    @Test
    void capturesWriterExceptionsAsFailedRepairs() {
        StoredRecord latest = record(2);
        ReadRepairPlan plan = new ReadRepairPlan(
                latest.key(),
                Optional.of(latest),
                List.of(new NodeId("node-1"))
        );
        ReadRepairExecutor executor = new ReadRepairExecutor((replica, mutation) -> {
            throw new IllegalStateException("repair write timeout");
        });

        ReadRepairResult result = executor.execute(plan, replicas(2));

        assertThat(result.failedRepairs()).isEqualTo(1);
        assertThat(result.responses())
                .singleElement()
                .satisfies(response -> assertThat(response.detail()).contains("repair write timeout"));
    }

    private static StoredRecord record(int version) {
        return new StoredRecord(
                Key.utf8("user:1"),
                Optional.of(new Value(new byte[] {(byte) version})),
                false,
                Instant.parse("2026-01-01T00:00:00Z").plusSeconds(version),
                Optional.empty(),
                "mutation-" + version
        );
    }

    private static List<ClusterNode> replicas(int replicaCount) {
        ArrayList<ClusterNode> replicas = new ArrayList<>();
        for (int index = 0; index < replicaCount; index++) {
            replicas.add(new ClusterNode(new NodeId("node-" + index), "host-" + index, 8080 + index, Map.of()));
        }
        return replicas;
    }
}
