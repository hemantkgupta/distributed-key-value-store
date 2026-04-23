package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.common.Value;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HttpCoordinatorClientTest {
    private static final Instant NOW = Instant.parse("2026-04-23T00:00:00Z");

    @TempDir
    Path tempDir;

    @Test
    void writesThroughCoordinatorEndpointToConfiguredReplicas() {
        try (KvNodeRuntime nodeB = KvNodeRuntime.start(KvNodeConfig.fromProperties(baseProperties("node-b", tempDir.resolve("node-b"))));
             KvNodeRuntime nodeA = KvNodeRuntime.start(KvNodeConfig.fromProperties(coordinatorProperties(
                     "node-a",
                     tempDir.resolve("node-a"),
                     nodeB.localNode().port()
             )))) {
            HttpCoordinatorClient client = new HttpCoordinatorClient();
            Key key = Key.utf8("user:1");
            MutationRecord mutation = put(key, "alice", 1);

            CoordinatorWriteResponse response = client.write(
                    nodeA.localNode(),
                    new CoordinatorWriteRequest(mutation, ConsistencyLevel.ALL)
            );

            assertThat(response.success()).isTrue();
            assertThat(response.acknowledgements()).isEqualTo(2);
            assertThat(nodeA.storage().get(key)).contains(StoredRecord.from(mutation));
            assertThat(nodeB.storage().get(key)).contains(StoredRecord.from(mutation));
        }
    }

    @Test
    void readsThroughCoordinatorEndpointAndRepairsStaleReplica() {
        try (KvNodeRuntime nodeB = KvNodeRuntime.start(KvNodeConfig.fromProperties(baseProperties("node-b", tempDir.resolve("node-b"))));
             KvNodeRuntime nodeA = KvNodeRuntime.start(KvNodeConfig.fromProperties(coordinatorProperties(
                     "node-a",
                     tempDir.resolve("node-a"),
                     nodeB.localNode().port()
             )))) {
            HttpCoordinatorClient client = new HttpCoordinatorClient();
            Key key = Key.utf8("user:1");
            MutationRecord stale = put(key, "alice-v1", 1);
            MutationRecord latest = put(key, "alice-v2", 2);
            nodeB.storage().apply(stale);
            nodeA.storage().apply(latest);

            CoordinatorReadResponse response = client.read(
                    nodeA.localNode(),
                    new CoordinatorReadRequest(key, ConsistencyLevel.ALL)
            );

            assertThat(response.success()).isTrue();
            assertThat(response.record()).contains(StoredRecord.from(latest));
            assertThat(response.digestsAgree()).isFalse();
            assertThat(response.successfulRepairs()).isEqualTo(1);
            assertThat(nodeB.storage().get(key)).contains(StoredRecord.from(latest));
        }
    }

    private static Properties baseProperties(String nodeId, Path storagePath) {
        Properties properties = new Properties();
        properties.setProperty(KvNodeConfig.NODE_ID_PROPERTY, nodeId);
        properties.setProperty(KvNodeConfig.HOST_PROPERTY, "127.0.0.1");
        properties.setProperty(KvNodeConfig.PORT_PROPERTY, "0");
        properties.setProperty(KvNodeConfig.STORAGE_PATH_PROPERTY, storagePath.toString());
        return properties;
    }

    private static Properties coordinatorProperties(String nodeId, Path storagePath, int otherPort) {
        Properties properties = baseProperties(nodeId, storagePath);
        properties.setProperty(CoordinatorConfig.REPLICA_COUNT_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.LOCAL_DATACENTER_PROPERTY, "dc-a");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "0.node-id", nodeId);
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "0.self", "true");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "0.datacenter", "dc-a");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "1.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "1.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "1.port", Integer.toString(otherPort));
        properties.setProperty(CoordinatorConfig.REPLICA_PREFIX + "1.datacenter", "dc-a");
        return properties;
    }

    private static MutationRecord put(Key key, String value, int version) {
        return new MutationRecord(
                key,
                Optional.of(new Value(value.getBytes(StandardCharsets.UTF_8))),
                false,
                NOW.plusSeconds(version),
                Optional.empty(),
                "m" + version
        );
    }
}
