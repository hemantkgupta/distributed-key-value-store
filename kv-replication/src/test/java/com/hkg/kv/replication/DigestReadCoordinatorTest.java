package com.hkg.kv.replication;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.storage.StoredRecord;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DigestReadCoordinatorTest {
    @Test
    void reportsAgreementWhenReplicaDigestsMatch() {
        ReplicationPlan plan = plan(3);
        DigestReadCoordinator coordinator = new DigestReadCoordinator((replica, key) ->
                ReplicaReadResponse.success(replica.nodeId(), Optional.of(record(key, 1)), new byte[] {7}));

        DigestReadResult result = coordinator.read(plan);

        assertThat(result.responses()).hasSize(3);
        assertThat(result.digestsAgree()).isTrue();
        assertThat(result.mismatchedResponses()).isEmpty();
    }

    @Test
    void reportsDigestMismatchAcrossSuccessfulResponses() {
        ReplicationPlan plan = plan(3);
        DigestReadCoordinator coordinator = new DigestReadCoordinator((replica, key) -> {
            byte digest = replica.nodeId().value().equals("node-2") ? (byte) 9 : (byte) 7;
            return ReplicaReadResponse.success(replica.nodeId(), Optional.of(record(key, digest)), new byte[] {digest});
        });

        DigestReadResult result = coordinator.read(plan);

        assertThat(result.digestsAgree()).isFalse();
        assertThat(result.mismatchedResponses())
                .singleElement()
                .satisfies(response -> assertThat(response.nodeId()).isEqualTo(new NodeId("node-2")));
    }

    @Test
    void capturesReadExceptionsAsFailedResponses() {
        ReplicationPlan plan = plan(3);
        DigestReadCoordinator coordinator = new DigestReadCoordinator((replica, key) -> {
            if (replica.nodeId().equals(new NodeId("node-1"))) {
                throw new IllegalStateException("read timeout");
            }
            return ReplicaReadResponse.success(replica.nodeId(), Optional.of(record(key, 1)), new byte[] {1});
        });

        DigestReadResult result = coordinator.read(plan);

        assertThat(result.successfulResponses()).hasSize(2);
        assertThat(result.failedResponses())
                .singleElement()
                .satisfies(response -> {
                    assertThat(response.nodeId()).isEqualTo(new NodeId("node-1"));
                    assertThat(response.detail()).contains("read timeout");
                });
    }

    @Test
    void treatsWrongResponseNodeIdAsFailure() {
        ReplicationPlan plan = plan(1);
        DigestReadCoordinator coordinator = new DigestReadCoordinator((replica, key) ->
                ReplicaReadResponse.success(new NodeId("wrong-node"), Optional.of(record(key, 1)), new byte[] {1}));

        DigestReadResult result = coordinator.read(plan);

        assertThat(result.failedResponses())
                .singleElement()
                .satisfies(response -> {
                    assertThat(response.nodeId()).isEqualTo(new NodeId("node-0"));
                    assertThat(response.detail()).contains("did not match");
                });
    }

    @Test
    void requestBudgetStopsLaterReplicaReads() {
        ReplicationPlan plan = plan(3);
        AtomicLong now = new AtomicLong();
        DigestReadCoordinator coordinator = new DigestReadCoordinator((replica, key) -> {
            now.addAndGet(5L);
            return ReplicaReadResponse.success(replica.nodeId(), Optional.of(record(key, 1)), new byte[] {1});
        });

        DigestReadResult result = coordinator.read(
                plan,
                RequestBudget.start(java.time.Duration.ofNanos(4L), now::get)
        );

        assertThat(result.successfulResponses()).hasSize(1);
        assertThat(result.failedResponses()).hasSize(2);
        assertThat(result.failedResponses())
                .allSatisfy(response -> assertThat(response.detail()).contains("before contacting replica"));
    }

    private static ReplicationPlan plan(int replicaCount) {
        List<ClusterNode> replicas = new ArrayList<>(replicaCount);
        for (int index = 0; index < replicaCount; index++) {
            replicas.add(new ClusterNode(new NodeId("node-" + index), "host-" + index, 8080 + index, Map.of()));
        }
        return new ReplicationPlan(Key.utf8("user:1"), replicas, ConsistencyLevel.QUORUM);
    }

    private static StoredRecord record(Key key, int version) {
        return new StoredRecord(
                key,
                Optional.of(new Value(new byte[] {(byte) version})),
                false,
                Instant.parse("2026-01-01T00:00:00Z").plusSeconds(version),
                Optional.empty(),
                "mutation-" + version
        );
    }
}
