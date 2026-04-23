package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.partitioning.KeyTokenHasher;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.repair.MerkleDifference;
import com.hkg.kv.repair.MerkleRepairPlan;
import com.hkg.kv.repair.MerkleRepairResult;
import com.hkg.kv.repair.RemoteMerkleRepairExecutor;
import com.hkg.kv.replication.ReplicaReadResponse;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StorageEngine;
import com.hkg.kv.storage.StoredRecord;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class HttpReplicaTransportClientTest {
    private static final Instant NOW = Instant.parse("2026-04-23T00:00:00Z");
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test
    void writesMutationThroughHttpHandler() throws Exception {
        try (RunningNode node = new RunningNode("node-a")) {
            HttpReplicaTransportClient client = client();
            Key key = Key.utf8("user:1");
            MutationRecord mutation = put(key, "alice", 1);

            ReplicaResponse response = client.write(node.clusterNode(), mutation);

            assertThat(response.success()).isTrue();
            assertThat(response.nodeId()).isEqualTo(node.clusterNode().nodeId());
            assertThat(node.get(key)).contains(StoredRecord.from(mutation));
        }
    }

    @Test
    void readsRecordAndDigestThroughHttpHandler() throws Exception {
        try (RunningNode node = new RunningNode("node-a")) {
            HttpReplicaTransportClient client = client();
            Key key = Key.utf8("user:1");
            MutationRecord mutation = put(key, "alice", 1);
            node.apply(mutation);

            ReplicaReadResponse response = client.read(node.clusterNode(), key);

            assertThat(response.success()).isTrue();
            assertThat(response.record()).contains(StoredRecord.from(mutation));
            assertThat(response.digest()).containsExactly(node.digest(key));
        }
    }

    @Test
    void streamsOnlyRequestedRangeRecords() throws Exception {
        try (RunningNode node = new RunningNode("node-a")) {
            HttpReplicaTransportClient client = client();
            List<TokenRange> ranges = TokenRange.fullRing().split();
            Key inRange = keyInRange(ranges.get(0), "left");
            Key outOfRange = keyInRange(ranges.get(1), "right");
            node.apply(put(inRange, "in-range", 1));
            node.apply(put(outOfRange, "out-of-range", 2));

            List<StoredRecord> records = client.stream(node.clusterNode(), ranges.get(0));

            assertThat(records).extracting(StoredRecord::key).containsExactly(inRange);
        }
    }

    @Test
    void repairsRemoteMerkleDifferenceThroughHttpTransport() throws Exception {
        try (RunningNode left = new RunningNode("node-left");
             RunningNode right = new RunningNode("node-right")) {
            Key key = Key.utf8("user:1");
            left.apply(put(key, "left", 1));
            HttpReplicaTransportClient client = client();
            RemoteMerkleRepairExecutor executor = new RemoteMerkleRepairExecutor(client, client);

            MerkleRepairResult result = executor.execute(
                    new MerkleRepairPlan(
                            TokenRange.fullRing(),
                            List.of(new MerkleDifference(TokenRange.fullRing(), 1, 0, new byte[] {1}, new byte[] {2}))
                    ),
                    left.clusterNode(),
                    right.clusterNode()
            );

            assertThat(result.appliedToRight()).isEqualTo(1);
            assertThat(result.failedWrites()).isZero();
            assertThat(right.get(key)).contains(left.get(key).orElseThrow());
        }
    }

    @Test
    void returnsFailureResponseWhenWriteEndpointReturnsErrorStatus() throws Exception {
        try (RawServer server = RawServer.withStatus(HttpReplicaTransportPaths.REPLICA_WRITE_PATH, 500, "boom")) {
            HttpReplicaTransportClient client = client();
            ClusterNode replica = server.clusterNode("node-a");

            ReplicaResponse response = client.write(replica, put(Key.utf8("user:1"), "alice", 1));

            assertThat(response.success()).isFalse();
            assertThat(response.detail()).contains("http status 500");
        }
    }

    @Test
    void returnsFailureResponseWhenReadEndpointDoesNotExist() throws Exception {
        try (RawServer server = RawServer.withStatus("/noop", 200, "ok")) {
            HttpReplicaTransportClient client = client();
            ClusterNode replica = server.clusterNode("node-a");

            ReplicaReadResponse response = client.read(replica, Key.utf8("user:1"));

            assertThat(response.success()).isFalse();
            assertThat(response.detail()).contains("http status 404");
        }
    }

    @Test
    void throwsWhenRangeStreamingEndpointReturnsErrorStatus() throws Exception {
        try (RawServer server = RawServer.withStatus(HttpReplicaTransportPaths.MERKLE_RANGE_STREAM_PATH, 500, "boom")) {
            HttpReplicaTransportClient client = client();
            ClusterNode replica = server.clusterNode("node-a");

            assertThatThrownBy(() -> client.stream(replica, TokenRange.fullRing()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("http status 500");
        }
    }

    private static HttpReplicaTransportClient client() {
        return new HttpReplicaTransportClient(HttpClient.newHttpClient(), TIMEOUT);
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

    private static Key keyInRange(TokenRange range, String prefix) {
        for (int index = 0; index < 10_000; index++) {
            Key key = Key.utf8(prefix + ":" + index);
            if (range.contains(KeyTokenHasher.tokenFor(key))) {
                return key;
            }
        }
        throw new IllegalStateException("unable to find key in range");
    }

    private static final class RunningNode implements AutoCloseable {
        private final InMemoryStorage storage = new InMemoryStorage();
        private final HttpServer server;
        private final ClusterNode clusterNode;

        private RunningNode(String nodeId) throws IOException {
            NodeId localNodeId = new NodeId(nodeId);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            HttpReplicaTransportHandlers handlers = new HttpReplicaTransportHandlers(localNodeId, storage);
            server.createContext(HttpReplicaTransportPaths.REPLICA_WRITE_PATH, handlers.replicaWriteHandler());
            server.createContext(HttpReplicaTransportPaths.REPLICA_READ_PATH, handlers.replicaReadHandler());
            server.createContext(HttpReplicaTransportPaths.MERKLE_RANGE_STREAM_PATH, handlers.merkleRangeStreamHandler());
            server.start();
            clusterNode = new ClusterNode(localNodeId, "127.0.0.1", server.getAddress().getPort(), Map.of());
        }

        private ClusterNode clusterNode() {
            return clusterNode;
        }

        private void apply(MutationRecord mutation) {
            storage.apply(mutation);
        }

        private Optional<StoredRecord> get(Key key) {
            return storage.get(key);
        }

        private byte[] digest(Key key) {
            return storage.digest(key);
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static final class RawServer implements AutoCloseable {
        private final HttpServer server;

        private RawServer(HttpServer server) {
            this.server = server;
        }

        private static RawServer withStatus(String path, int statusCode, String body) throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext(path, exchange -> {
                byte[] responseBody = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(statusCode, responseBody.length);
                exchange.getResponseBody().write(responseBody);
                exchange.close();
            });
            server.start();
            return new RawServer(server);
        }

        private ClusterNode clusterNode(String nodeId) {
            return new ClusterNode(
                    new NodeId(nodeId),
                    "127.0.0.1",
                    server.getAddress().getPort(),
                    Map.of()
            );
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static final class InMemoryStorage implements StorageEngine {
        private final Map<Key, StoredRecord> records = new LinkedHashMap<>();

        @Override
        public void apply(MutationRecord mutation) {
            StoredRecord candidate = StoredRecord.from(mutation);
            StoredRecord existing = records.get(mutation.key());
            if (existing == null || candidate.compareVersionTo(existing) >= 0) {
                records.put(mutation.key(), candidate);
            }
        }

        @Override
        public Optional<StoredRecord> get(Key key) {
            return Optional.ofNullable(records.get(key));
        }

        @Override
        public List<StoredRecord> scanAll() {
            return new ArrayList<>(records.values());
        }

        @Override
        public byte[] digest(Key key) {
            return get(key)
                    .map(InMemoryStorage::recordDigest)
                    .orElseGet(() -> digest(new byte[0]));
        }

        @Override
        public void close() {
        }

        private static byte[] digest(byte[] bytes) {
            try {
                return MessageDigest.getInstance("SHA-256").digest(bytes);
            } catch (NoSuchAlgorithmException exception) {
                throw new IllegalStateException("SHA-256 is required by the JDK", exception);
            }
        }

        private static byte[] recordDigest(StoredRecord record) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                digest.update(record.mutationId().getBytes(StandardCharsets.UTF_8));
                digest.update((byte) (record.tombstone() ? 1 : 0));
                record.value().ifPresent(value -> digest.update(value.bytes()));
                return digest.digest();
            } catch (NoSuchAlgorithmException exception) {
                throw new IllegalStateException("SHA-256 is required by the JDK", exception);
            }
        }
    }
}
