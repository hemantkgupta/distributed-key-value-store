package com.hkg.kv.partitioning;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsistentHashReplicaPlacementPolicyTest {
    @Test
    void placementIsDeterministicForTheSameKey() {
        var policy = policy(32, 7L);
        var key = Key.utf8("customer:123");

        assertThat(policy.replicasFor(key, 3)).isEqualTo(policy.replicasFor(key, 3));
    }

    @Test
    void replicasAreDistinctPhysicalOwnersEvenWithManyVnodes() {
        var policy = policy(64, 11L);

        var replicas = policy.replicasFor(Key.utf8("hot-key"), 3);
        var replicaNodeIds = replicas.stream().map(ClusterNode::nodeId).collect(Collectors.toList());

        assertThat(replicaNodeIds).hasSize(3);
        assertThat(replicaNodeIds).doesNotHaveDuplicates();
    }

    @Test
    void wrapsAroundTheRingWhenTokenFallsPastTheEnd() {
        var snapshot = PartitionRingSnapshot.of(nodes(), 4, 19L);

        var replicas = snapshot.replicasFor(new Token(-1L), 2);

        assertThat(replicas).containsExactly(
                snapshot.vnodes().get(0).owner(),
                firstDistinctOwnerAfter(snapshot, snapshot.vnodes().get(0).owner())
        );
    }

    @Test
    void preservesEpochMetadata() {
        var snapshot = PartitionRingSnapshot.of(nodes(), 8, 1234L);

        assertThat(snapshot.epoch()).isEqualTo(1234L);
        assertThat(snapshot.withEpoch(4321L).epoch()).isEqualTo(4321L);
        assertThat(snapshot.withEpoch(4321L).vnodes()).isEqualTo(snapshot.vnodes());
    }

    @Test
    void invalidReplicationFactorIsRejected() {
        var policy = policy(8, 5L);

        assertThatThrownBy(() -> policy.replicasFor(Key.utf8("x"), 0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> policy.replicasFor(Key.utf8("x"), 5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nodeRemovalChangesOnlyASubsetOfPlacements() {
        var original = policy(24, 17L);
        var removed = ConsistentHashReplicaPlacementPolicy.of(nodes().subList(0, 3), 24, 18L);

        var keys = IntStream.range(0, 64)
                .mapToObj(i -> Key.utf8("sample-" + i))
                .toList();

        long unchangedPrimaries = keys.stream()
                .filter(key -> original.replicasFor(key, 3).get(0).equals(removed.replicasFor(key, 3).get(0)))
                .count();

        long changedPrimaries = keys.stream()
                .filter(key -> !original.replicasFor(key, 3).get(0).equals(removed.replicasFor(key, 3).get(0)))
                .count();

        assertThat(unchangedPrimaries).isGreaterThan(0);
        assertThat(changedPrimaries).isGreaterThan(0);
    }

    @Test
    void validatesEmptyNodesVnodeCountAndReplicationFactor() {
        assertThatThrownBy(() -> PartitionRingSnapshot.of(List.of(), 1, 1L))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> PartitionRingSnapshot.of(nodes(), 0, 1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private ConsistentHashReplicaPlacementPolicy policy(int vnodeCount, long epoch) {
        return ConsistentHashReplicaPlacementPolicy.of(nodes(), vnodeCount, epoch);
    }

    private List<ClusterNode> nodes() {
        return List.of(
                new ClusterNode(new NodeId("node-a"), "10.0.0.1", 7000, java.util.Map.of()),
                new ClusterNode(new NodeId("node-b"), "10.0.0.2", 7000, java.util.Map.of()),
                new ClusterNode(new NodeId("node-c"), "10.0.0.3", 7000, java.util.Map.of()),
                new ClusterNode(new NodeId("node-d"), "10.0.0.4", 7000, java.util.Map.of())
        );
    }

    private ClusterNode firstDistinctOwnerAfter(PartitionRingSnapshot snapshot, ClusterNode owner) {
        for (Vnode vnode : snapshot.vnodes()) {
            if (!vnode.owner().equals(owner)) {
                return vnode.owner();
            }
        }
        throw new IllegalStateException("expected at least one distinct owner");
    }
}
