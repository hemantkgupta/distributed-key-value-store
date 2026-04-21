package com.hkg.kv.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.storage.MutationRecord;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReplicationCoordinatorTest {
    @Test
    void quorumSucceedsWhenEnoughReplicasAcknowledge() {
        ReplicationPlan plan = plan(ConsistencyLevel.QUORUM, 5);
        ReplicationCoordinator coordinator = new ReplicationCoordinator((replica, mutation) ->
                isSuccessfulReplica(replica, 3)
                        ? success(replica)
                        : failure(replica, "write rejected"));

        ReplicationResult result = coordinator.replicate(plan, mutation(plan.key()));

        assertThat(result.successfulAcknowledgements()).isEqualTo(3);
        assertThat(result.failedResponses()).hasSize(2);
        assertThat(result.consistencySatisfied()).isTrue();
        assertThat(result.responses()).hasSize(5);
    }

    @Test
    void allFailsWhenAnyReplicaFails() {
        ReplicationPlan plan = plan(ConsistencyLevel.ALL, 3);
        ReplicationCoordinator coordinator = new ReplicationCoordinator((replica, mutation) ->
                isSuccessfulReplica(replica, 2)
                        ? success(replica)
                        : failure(replica, "disk full"));

        ReplicationResult result = coordinator.replicate(plan, mutation(plan.key()));

        assertThat(result.successfulAcknowledgements()).isEqualTo(2);
        assertThat(result.failedResponses()).hasSize(1);
        assertThat(result.consistencySatisfied()).isFalse();
    }

    @Test
    void fansOutToAllReplicasEvenAfterQuorumIsReached() {
        ReplicationPlan plan = plan(ConsistencyLevel.QUORUM, 5);
        AtomicInteger invocationCount = new AtomicInteger();
        ReplicationCoordinator coordinator = new ReplicationCoordinator((replica, mutation) -> {
            invocationCount.incrementAndGet();
            return success(replica);
        });

        ReplicationResult result = coordinator.replicate(plan, mutation(plan.key()));

        assertThat(invocationCount.get()).isEqualTo(5);
        assertThat(result.responses()).hasSize(5);
        assertThat(result.successfulAcknowledgements()).isEqualTo(5);
        assertThat(result.consistencySatisfied()).isTrue();
    }

    @Test
    void rejectsMutationKeyMismatch() {
        ReplicationPlan plan = plan(ConsistencyLevel.ONE, 1);
        ReplicationCoordinator coordinator = new ReplicationCoordinator((replica, mutation) -> success(replica));

        assertThatThrownBy(() -> coordinator.replicate(plan, mutation(Key.utf8("other"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("mutation key must match replication plan key");
    }

    @Test
    void capturesWriterExceptionsAsFailedResponses() {
        ReplicationPlan plan = plan(ConsistencyLevel.QUORUM, 3);
        ReplicationCoordinator coordinator = new ReplicationCoordinator((replica, mutation) -> {
            if (replica.nodeId().equals(plan.replicas().get(1).nodeId())) {
                throw new IllegalStateException("transport unavailable");
            }
            return success(replica);
        });

        ReplicationResult result = coordinator.replicate(plan, mutation(plan.key()));

        assertThat(result.responses()).hasSize(3);
        assertThat(result.failedResponses())
                .singleElement()
                .satisfies(response -> {
                    assertThat(response.nodeId()).isEqualTo(plan.replicas().get(1).nodeId());
                    assertThat(response.success()).isFalse();
                    assertThat(response.detail()).contains("IllegalStateException");
                    assertThat(response.detail()).contains("transport unavailable");
                });
        assertThat(result.successfulAcknowledgements()).isEqualTo(2);
        assertThat(result.consistencySatisfied()).isTrue();
    }

    @Test
    void localQuorumUsesLocalReplicaCountForThresholding() {
        ReplicationPlan plan = plan(ConsistencyLevel.LOCAL_QUORUM, 5);
        ReplicationCoordinator coordinator = new ReplicationCoordinator((replica, mutation) ->
                isSuccessfulReplica(replica, 2)
                        ? success(replica)
                        : failure(replica, "local replica unavailable"));

        ReplicationResult result = coordinator.replicate(plan, mutation(plan.key()), 3);

        assertThat(result.waitPolicy().acknowledgementsRequired()).isEqualTo(2);
        assertThat(result.successfulAcknowledgements()).isEqualTo(2);
        assertThat(result.consistencySatisfied()).isTrue();
    }

    private static ReplicationPlan plan(ConsistencyLevel consistencyLevel, int replicaCount) {
        return new ReplicationPlan(
                Key.utf8("user:1"),
                replicas(replicaCount),
                consistencyLevel
        );
    }

    private static List<ClusterNode> replicas(int replicaCount) {
        List<ClusterNode> replicas = new ArrayList<>(replicaCount);
        for (int index = 0; index < replicaCount; index++) {
            replicas.add(new ClusterNode(new NodeId("node-" + index), "host-" + index, 8080 + index, java.util.Map.of()));
        }
        return replicas;
    }

    private static MutationRecord mutation(Key key) {
        return new MutationRecord(
                key,
                Optional.of(new Value(new byte[] {1})),
                false,
                Instant.parse("2026-01-01T00:00:00Z"),
                Optional.empty(),
                "mutation-1"
        );
    }

    private static ReplicaResponse success(ClusterNode replica) {
        return new ReplicaResponse(replica.nodeId(), true, "ok");
    }

    private static ReplicaResponse failure(ClusterNode replica, String detail) {
        return new ReplicaResponse(replica.nodeId(), false, detail);
    }

    private static boolean isSuccessfulReplica(ClusterNode replica, int successfulReplicaCount) {
        int index = Integer.parseInt(replica.nodeId().value().substring("node-".length()));
        return index < successfulReplicaCount;
    }
}
