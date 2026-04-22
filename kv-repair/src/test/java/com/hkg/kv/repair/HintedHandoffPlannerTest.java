package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.replication.ConsistencyWaitPolicy;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.replication.ReplicationPlan;
import com.hkg.kv.replication.ReplicationResult;
import com.hkg.kv.storage.MutationRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class HintedHandoffPlannerTest {
    @Test
    void recordsHintsForFailedReplicaResponses() {
        InMemoryHintStore store = new InMemoryHintStore();
        AtomicInteger hintIds = new AtomicInteger();
        HintedHandoffService service = new HintedHandoffService(
                store,
                () -> "hint-" + hintIds.incrementAndGet(),
                Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC)
        );
        HintedHandoffPlanner planner = new HintedHandoffPlanner(service);
        ReplicationPlan plan = plan();
        MutationRecord mutation = mutation(plan.key());
        ReplicationResult result = new ReplicationResult(
                List.of(
                        new ReplicaResponse(new NodeId("node-0"), true, "ok"),
                        new ReplicaResponse(new NodeId("node-1"), false, "timeout"),
                        new ReplicaResponse(new NodeId("node-2"), false, "unavailable")
                ),
                1,
                ConsistencyWaitPolicy.forLevel(ConsistencyLevel.ONE, 3, null),
                true
        );

        List<HintRecord> hints = planner.recordFailedReplicas(plan, mutation, result);

        assertThat(hints).extracting(HintRecord::hintId).containsExactly("hint-1", "hint-2");
        assertThat(hints).extracting(HintRecord::targetNodeId)
                .containsExactly(new NodeId("node-1"), new NodeId("node-2"));
        assertThat(store.loadAll()).hasSize(2);
    }

    @Test
    void rejectsFailedResponsesOutsidePlan() {
        HintedHandoffPlanner planner = new HintedHandoffPlanner(new HintedHandoffService(
                new InMemoryHintStore(),
                () -> "hint-1",
                Clock.systemUTC()
        ));
        ReplicationPlan plan = plan();
        ReplicationResult result = new ReplicationResult(
                List.of(new ReplicaResponse(new NodeId("unknown-node"), false, "timeout")),
                0,
                ConsistencyWaitPolicy.forLevel(ConsistencyLevel.ONE, 3, null),
                false
        );

        assertThatThrownBy(() -> planner.recordFailedReplicas(plan, mutation(plan.key()), result))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("failed response did not belong to replication plan");
    }

    private static ReplicationPlan plan() {
        return new ReplicationPlan(
                Key.utf8("user:1"),
                List.of(
                        replica(0),
                        replica(1),
                        replica(2)
                ),
                ConsistencyLevel.ONE
        );
    }

    private static ClusterNode replica(int index) {
        return new ClusterNode(new NodeId("node-" + index), "host-" + index, 8080 + index, Map.of());
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

    private static final class InMemoryHintStore implements HintStore {
        private final List<HintRecord> hints = new ArrayList<>();

        @Override
        public void append(HintRecord hint) {
            hints.add(hint);
        }

        @Override
        public List<HintRecord> loadAll() {
            return List.copyOf(hints);
        }

        @Override
        public void remove(String hintId) {
            hints.removeIf(hint -> hint.hintId().equals(hintId));
        }
    }
}
