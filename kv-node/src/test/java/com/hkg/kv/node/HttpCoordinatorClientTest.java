package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.common.Value;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StoredRecord;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
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
    void writesThroughCoordinatorEndpointToRingPlannedReplicas() {
        try (KvNodeRuntime nodeB = KvNodeRuntime.start(KvNodeConfig.fromProperties(baseProperties("node-b", tempDir.resolve("node-b"))));
             KvNodeRuntime nodeA = KvNodeRuntime.start(KvNodeConfig.fromProperties(replicatedRingProperties(
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
            assertThat(response.durableHintsRecorded()).isZero();
            assertThat(nodeA.storage().get(key)).contains(StoredRecord.from(mutation));
            assertThat(nodeB.storage().get(key)).contains(StoredRecord.from(mutation));
        }
    }

    @Test
    void readsThroughCoordinatorEndpointAndRepairsStaleReplicaFromRingPlan() {
        try (KvNodeRuntime nodeB = KvNodeRuntime.start(KvNodeConfig.fromProperties(baseProperties("node-b", tempDir.resolve("node-b"))));
             KvNodeRuntime nodeA = KvNodeRuntime.start(KvNodeConfig.fromProperties(replicatedRingProperties(
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

    @Test
    void writesAtAnyAndPersistsHintWhenRingReplicaIsUnavailable() {
        int unavailablePort;
        try (KvNodeRuntime nodeB = KvNodeRuntime.start(KvNodeConfig.fromProperties(baseProperties("node-b", tempDir.resolve("offline-node"))))) {
            unavailablePort = nodeB.localNode().port();
        }

        try (KvNodeRuntime nodeA = KvNodeRuntime.start(KvNodeConfig.fromProperties(hintOnlyRingProperties(
                "node-a",
                tempDir.resolve("node-a"),
                unavailablePort
        )))) {
            HttpCoordinatorClient client = new HttpCoordinatorClient();
            Key key = Key.utf8("user:hinted");
            MutationRecord mutation = put(key, "queued-for-hint", 3);

            CoordinatorWriteResponse response = client.write(
                    nodeA.localNode(),
                    new CoordinatorWriteRequest(mutation, ConsistencyLevel.ANY)
            );

            assertThat(response.success()).isTrue();
            assertThat(response.acknowledgements()).isZero();
            assertThat(response.acknowledgementsRequired()).isEqualTo(1);
            assertThat(response.durableHintsRecorded()).isEqualTo(1);
            assertThat(nodeA.storage().get(key)).isEmpty();
            assertThat(nodeA.hintStore().loadAll())
                    .singleElement()
                    .satisfies(hint -> {
                        assertThat(hint.targetNodeId().value()).isEqualTo("node-b");
                        assertThat(hint.mutation()).isEqualTo(mutation);
                    });
        }
    }

    @Test
    void writeBudgetStopsLaterStaticReplicaAttempts() {
        try (SlowServer slowServer = SlowServer.start(Duration.ofMillis(250));
             KvNodeRuntime nodeA = KvNodeRuntime.start(KvNodeConfig.fromProperties(staticBudgetProperties(
                     "node-a",
                     tempDir.resolve("node-a-budget"),
                     slowServer.port()
             )))) {
            HttpCoordinatorClient client = new HttpCoordinatorClient();
            Key key = Key.utf8("user:budget");
            MutationRecord mutation = put(key, "should-not-reach-self", 4);

            CoordinatorWriteResponse response = client.write(
                    nodeA.localNode(),
                    new CoordinatorWriteRequest(mutation, ConsistencyLevel.ONE)
            );

            assertThat(response.success()).isFalse();
            assertThat(response.acknowledgements()).isZero();
            assertThat(response.totalAttempts()).isEqualTo(1);
            assertThat(response.failedReplicaDetails())
                    .anySatisfy(detail -> assertThat(detail).contains("request budget exhausted before contacting replica"));
            assertThat(nodeA.storage().get(key)).isEmpty();
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

    private static Properties replicatedRingProperties(String nodeId, Path storagePath, int otherPort) {
        Properties properties = baseProperties(nodeId, storagePath);
        properties.setProperty(CoordinatorConfig.NODE_COUNT_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.LOCAL_DATACENTER_PROPERTY, "dc-a");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.node-id", nodeId);
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.self", "true");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.datacenter", "dc-a");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.port", Integer.toString(otherPort));
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.datacenter", "dc-a");
        properties.setProperty(CoordinatorConfig.REPLICATION_FACTOR_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.VNODE_COUNT_PROPERTY, "16");
        properties.setProperty(CoordinatorConfig.RING_EPOCH_PROPERTY, "7");
        return properties;
    }

    private static Properties hintOnlyRingProperties(String nodeId, Path storagePath, int unavailablePort) {
        Properties properties = baseProperties(nodeId, storagePath);
        properties.setProperty(CoordinatorConfig.NODE_COUNT_PROPERTY, "1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.port", Integer.toString(unavailablePort));
        properties.setProperty(CoordinatorConfig.REPLICATION_FACTOR_PROPERTY, "1");
        properties.setProperty(CoordinatorConfig.VNODE_COUNT_PROPERTY, "8");
        properties.setProperty(CoordinatorConfig.RING_EPOCH_PROPERTY, "5");
        properties.setProperty(KvNodeConfig.REQUEST_TIMEOUT_PROPERTY, "PT0.25S");
        return properties;
    }

    private static Properties staticBudgetProperties(String nodeId, Path storagePath, int unavailablePort) {
        Properties properties = baseProperties(nodeId, storagePath);
        properties.setProperty(CoordinatorConfig.NODE_COUNT_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.port", Integer.toString(unavailablePort));
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.node-id", nodeId);
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.self", "true");
        properties.setProperty(CoordinatorConfig.MAX_ATTEMPTS_PROPERTY, "3");
        properties.setProperty(CoordinatorConfig.REQUEST_BUDGET_PROPERTY, "PT0.05S");
        properties.setProperty(KvNodeConfig.REQUEST_TIMEOUT_PROPERTY, "PT0.25S");
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

    private static final class SlowServer implements AutoCloseable {
        private final HttpServer server;

        private SlowServer(HttpServer server) {
            this.server = server;
        }

        static SlowServer start(Duration responseDelay) {
            try {
                HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                server.createContext("/", exchange -> {
                    try {
                        Thread.sleep(responseDelay.toMillis());
                        exchange.sendResponseHeaders(503, -1);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        exchange.sendResponseHeaders(500, -1);
                    } finally {
                        exchange.close();
                    }
                });
                server.start();
                return new SlowServer(server);
            } catch (IOException exception) {
                throw new IllegalStateException("failed to start slow server", exception);
            }
        }

        int port() {
            return server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
