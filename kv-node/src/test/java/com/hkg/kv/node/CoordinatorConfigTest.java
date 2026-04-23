package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.replication.ReplicationOptions;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class CoordinatorConfigTest {
    @Test
    void defaultsToLocalOnlyRouting() {
        CoordinatorConfig config = CoordinatorConfig.fromProperties(new Properties());
        ClusterNode localNode = new ClusterNode(new NodeId("node-a"), "127.0.0.1", 9042, java.util.Map.of());

        List<ClusterNode> replicas = config.resolveReplicas(localNode);

        assertThat(config).isEqualTo(CoordinatorConfig.localOnly());
        assertThat(replicas).containsExactly(localNode);
    }

    @Test
    void parsesReplicaEntriesAndLocalDatacenter() {
        Properties properties = new Properties();
        properties.setProperty(CoordinatorConfig.REPLICA_COUNT_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.LOCAL_DATACENTER_PROPERTY, "dc-a");
        properties.setProperty(CoordinatorConfig.MAX_ATTEMPTS_PROPERTY, "3");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "0.node-id", "node-a");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "0.self", "true");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "0.datacenter", "dc-a");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "1.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "1.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "1.port", "9043");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "1.datacenter", "dc-a");
        ClusterNode localNode = new ClusterNode(new NodeId("node-a"), "127.0.0.1", 9042, java.util.Map.of());

        CoordinatorConfig config = CoordinatorConfig.fromProperties(properties);
        List<ClusterNode> replicas = config.resolveReplicas(localNode);

        assertThat(config.maxAttempts()).isEqualTo(3);
        assertThat(config.localDatacenter()).isEqualTo("dc-a");
        assertThat(replicas).hasSize(2);
        assertThat(replicas.get(0).port()).isEqualTo(9042);
        assertThat(replicas.get(0).labels()).containsEntry(ReplicationOptions.DATACENTER_LABEL, "dc-a");
        assertThat(replicas.get(1).nodeId().value()).isEqualTo("node-b");
    }

    @Test
    void rejectsNonSelfReplicaWithoutHost() {
        Properties properties = new Properties();
        properties.setProperty(CoordinatorConfig.REPLICA_COUNT_PROPERTY, "1");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "0.node-id", "node-a");

        assertThatThrownBy(() -> CoordinatorConfig.fromProperties(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host is required");
    }

    @Test
    void rejectsSelfReplicaThatDoesNotMatchLocalNode() {
        Properties properties = new Properties();
        properties.setProperty(CoordinatorConfig.REPLICA_COUNT_PROPERTY, "1");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "0.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "0.self", "true");
        CoordinatorConfig config = CoordinatorConfig.fromProperties(properties);
        ClusterNode localNode = new ClusterNode(new NodeId("node-a"), "127.0.0.1", 9042, java.util.Map.of());

        assertThatThrownBy(() -> config.resolveReplicas(localNode))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must match local node id");
    }
}
