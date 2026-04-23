package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.replication.ReplicationOptions;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CoordinatorConfigTest {
    @TempDir
    Path tempDir;

    @Test
    void defaultsToLocalOnlyRouting() {
        CoordinatorConfig config = CoordinatorConfig.fromProperties(new Properties());
        ClusterNode localNode = new ClusterNode(new NodeId("node-a"), "127.0.0.1", 9042, java.util.Map.of());

        List<ClusterNode> nodes = config.resolveClusterNodes(localNode);

        assertThat(config).isEqualTo(CoordinatorConfig.localOnly());
        assertThat(config.ringDrivenPlanningEnabled()).isFalse();
        assertThat(nodes).containsExactly(localNode);
        assertThat(config.createReplicaPlanner(localNode).plan(Key.utf8("customer:1"), ConsistencyLevel.ALL).replicas())
                .containsExactly(localNode);
        assertThat(config.resolveHintStorePath(tempDir.resolve("rocksdb")))
                .isEqualTo(tempDir.resolve("rocksdb").resolve("coordinator-hints.log"));
    }

    @Test
    void parsesStaticNodeEntriesAndLocalDatacenter() {
        Properties properties = new Properties();
        properties.setProperty(CoordinatorConfig.NODE_COUNT_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.LOCAL_DATACENTER_PROPERTY, "dc-a");
        properties.setProperty(CoordinatorConfig.MAX_ATTEMPTS_PROPERTY, "3");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.node-id", "node-a");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.self", "true");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.datacenter", "dc-a");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.port", "9043");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.datacenter", "dc-a");
        ClusterNode localNode = new ClusterNode(new NodeId("node-a"), "127.0.0.1", 9042, java.util.Map.of());

        CoordinatorConfig config = CoordinatorConfig.fromProperties(properties);
        List<ClusterNode> nodes = config.resolveClusterNodes(localNode);

        assertThat(config.maxAttempts()).isEqualTo(3);
        assertThat(config.localDatacenter()).isEqualTo("dc-a");
        assertThat(config.ringDrivenPlanningEnabled()).isFalse();
        assertThat(nodes).hasSize(2);
        assertThat(nodes.get(0).port()).isEqualTo(9042);
        assertThat(nodes.get(0).labels()).containsEntry(ReplicationOptions.DATACENTER_LABEL, "dc-a");
        assertThat(nodes.get(1).nodeId().value()).isEqualTo("node-b");
        assertThat(config.createReplicaPlanner(localNode).plan(Key.utf8("customer:2"), ConsistencyLevel.ALL).replicas())
                .containsExactlyElementsOf(nodes);
    }

    @Test
    void parsesRingPlanningAndHintSettings() {
        Properties properties = new Properties();
        properties.setProperty(CoordinatorConfig.NODE_COUNT_PROPERTY, "3");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.node-id", "node-a");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.self", "true");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.port", "9043");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "2.node-id", "node-c");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "2.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "2.port", "9044");
        properties.setProperty(CoordinatorConfig.REPLICATION_FACTOR_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.VNODE_COUNT_PROPERTY, "64");
        properties.setProperty(CoordinatorConfig.RING_EPOCH_PROPERTY, "11");
        properties.setProperty(CoordinatorConfig.HINTED_HANDOFF_ENABLED_PROPERTY, "false");
        properties.setProperty(CoordinatorConfig.HINT_STORE_PATH_PROPERTY, tempDir.resolve("pending-hints.log").toString());
        ClusterNode localNode = new ClusterNode(new NodeId("node-a"), "127.0.0.1", 9042, java.util.Map.of());

        CoordinatorConfig config = CoordinatorConfig.fromProperties(properties);
        List<ClusterNode> plannedReplicas = config.createReplicaPlanner(localNode)
                .plan(Key.utf8("hot-key"), ConsistencyLevel.QUORUM)
                .replicas();

        assertThat(config.ringDrivenPlanningEnabled()).isTrue();
        assertThat(config.replicationFactor()).isEqualTo(2);
        assertThat(config.vnodeCount()).isEqualTo(64);
        assertThat(config.ringEpoch()).isEqualTo(11L);
        assertThat(config.hintedHandoffEnabled()).isFalse();
        assertThat(config.resolveHintStorePath(tempDir.resolve("ignored"))).isEqualTo(tempDir.resolve("pending-hints.log"));
        assertThat(plannedReplicas).hasSize(2);
        assertThat(plannedReplicas.stream().map(node -> node.nodeId().value()).distinct().count()).isEqualTo(2);
    }

    @Test
    void rejectsNonSelfReplicaWithoutHost() {
        Properties properties = new Properties();
        properties.setProperty(CoordinatorConfig.NODE_COUNT_PROPERTY, "1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.node-id", "node-a");

        assertThatThrownBy(() -> CoordinatorConfig.fromProperties(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host is required");
    }

    @Test
    void rejectsSelfReplicaThatDoesNotMatchLocalNode() {
        Properties properties = new Properties();
        properties.setProperty(CoordinatorConfig.NODE_COUNT_PROPERTY, "1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.self", "true");
        CoordinatorConfig config = CoordinatorConfig.fromProperties(properties);
        ClusterNode localNode = new ClusterNode(new NodeId("node-a"), "127.0.0.1", 9042, java.util.Map.of());

        assertThatThrownBy(() -> config.resolveClusterNodes(localNode))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must match local node id");
    }

    @Test
    void rejectsRingReplicationFactorLargerThanConfiguredCluster() {
        Properties properties = new Properties();
        properties.setProperty(CoordinatorConfig.NODE_COUNT_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.node-id", "node-a");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.self", "true");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.port", "9043");
        properties.setProperty(CoordinatorConfig.REPLICATION_FACTOR_PROPERTY, "3");
        CoordinatorConfig config = CoordinatorConfig.fromProperties(properties);
        ClusterNode localNode = new ClusterNode(new NodeId("node-a"), "127.0.0.1", 9042, java.util.Map.of());

        assertThatThrownBy(() -> config.createReplicaPlanner(localNode))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("replication factor must not exceed configured cluster nodes");
    }
}
