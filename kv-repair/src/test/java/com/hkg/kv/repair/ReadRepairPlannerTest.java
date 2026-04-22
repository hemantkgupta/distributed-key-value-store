package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.replication.DigestReadResult;
import com.hkg.kv.replication.ReplicaReadResponse;
import com.hkg.kv.storage.StoredRecord;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ReadRepairPlannerTest {
    @Test
    void returnsEmptyPlanWhenDigestsAgree() {
        Key key = Key.utf8("user:1");
        StoredRecord record = record(key, 1);
        DigestReadResult result = new DigestReadResult(
                key,
                List.of(
                        ReplicaReadResponse.success(new NodeId("node-0"), Optional.of(record), new byte[] {1}),
                        ReplicaReadResponse.success(new NodeId("node-1"), Optional.of(record), new byte[] {1})
                )
        );

        ReadRepairPlan plan = new ReadRepairPlanner().plan(result);

        assertThat(plan.requiresRepair()).isFalse();
        assertThat(plan.targetNodeIds()).isEmpty();
    }

    @Test
    void targetsStaleSuccessfulReplicasForRepair() {
        Key key = Key.utf8("user:1");
        StoredRecord stale = record(key, 1);
        StoredRecord latest = record(key, 2);
        DigestReadResult result = new DigestReadResult(
                key,
                List.of(
                        ReplicaReadResponse.success(new NodeId("node-0"), Optional.of(stale), new byte[] {1}),
                        ReplicaReadResponse.success(new NodeId("node-1"), Optional.of(latest), new byte[] {2}),
                        ReplicaReadResponse.failure(new NodeId("node-2"), "timeout")
                )
        );

        ReadRepairPlan plan = new ReadRepairPlanner().plan(result);

        assertThat(plan.requiresRepair()).isTrue();
        assertThat(plan.latestRecord()).contains(latest);
        assertThat(plan.targetNodeIds()).containsExactly(new NodeId("node-0"));
    }

    @Test
    void handlesTombstoneAsLatestRecord() {
        Key key = Key.utf8("user:1");
        StoredRecord live = record(key, 1);
        StoredRecord tombstone = new StoredRecord(
                key,
                Optional.empty(),
                true,
                Instant.parse("2026-01-01T00:00:02Z"),
                Optional.empty(),
                "mutation-2"
        );
        DigestReadResult result = new DigestReadResult(
                key,
                List.of(
                        ReplicaReadResponse.success(new NodeId("node-0"), Optional.of(live), new byte[] {1}),
                        ReplicaReadResponse.success(new NodeId("node-1"), Optional.of(tombstone), new byte[] {2})
                )
        );

        ReadRepairPlan plan = new ReadRepairPlanner().plan(result);

        assertThat(plan.latestRecord()).contains(tombstone);
        assertThat(plan.targetNodeIds()).containsExactly(new NodeId("node-0"));
    }

    @Test
    void returnsEmptyPlanWhenNoSuccessfulResponseCarriesARecord() {
        Key key = Key.utf8("missing");
        DigestReadResult result = new DigestReadResult(
                key,
                List.of(ReplicaReadResponse.failure(new NodeId("node-0"), "timeout"))
        );

        ReadRepairPlan plan = new ReadRepairPlanner().plan(result);

        assertThat(plan.requiresRepair()).isFalse();
        assertThat(plan.latestRecord()).isEmpty();
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
