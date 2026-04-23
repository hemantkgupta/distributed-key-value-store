package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.repair.HintRecord;
import com.hkg.kv.repair.HintReplaySummary;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.repair.MerkleRepairLease;
import com.hkg.kv.replication.ReplicaReadResponse;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StoredRecord;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KvNodeRuntimeTest {
    private static final AtomicInteger DATABASE_ID = new AtomicInteger();
    private static final Instant NOW = Instant.parse("2026-04-23T00:00:00Z");

    @TempDir
    Path tempDir;

    @Test
    void startsEmbeddedNodeAndServesReplicaEndpoints() {
        Properties properties = baseProperties(tempDir.resolve("rocksdb-runtime"));

        try (KvNodeRuntime runtime = KvNodeRuntime.start(KvNodeConfig.fromProperties(properties))) {
            Key key = Key.utf8("user:1");
            MutationRecord mutation = put(key, "alice", 1);

            ReplicaResponse writeResponse = runtime.transportClient().write(runtime.localNode(), mutation);
            ReplicaReadResponse readResponse = runtime.transportClient().read(runtime.localNode(), key);
            List<StoredRecord> records = runtime.transportClient().stream(runtime.localNode(), TokenRange.fullRing());

            assertThat(runtime.localNode().nodeId().value()).isEqualTo("node-a");
            assertThat(runtime.localNode().port()).isPositive();
            assertThat(writeResponse.success()).isTrue();
            assertThat(readResponse.success()).isTrue();
            assertThat(readResponse.record()).contains(StoredRecord.from(mutation));
            assertThat(records).containsExactly(StoredRecord.from(mutation));
        }
    }

    @Test
    void wiresJdbcLeaseBackendIntoRuntimeLifecycle() {
        Properties properties = baseProperties(tempDir.resolve("rocksdb-jdbc"));
        properties.setProperty(RepairLeaseStoreConfig.BACKEND_PROPERTY, "jdbc");
        properties.setProperty(
                RepairLeaseStoreConfig.JDBC_URL_PROPERTY,
                "jdbc:h2:mem:kv-node-runtime-" + DATABASE_ID.incrementAndGet() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
        );

        try (KvNodeRuntime runtime = KvNodeRuntime.start(KvNodeConfig.fromProperties(properties))) {
            Optional<MerkleRepairLease> lease = runtime.repairLeaseStore().tryAcquire(
                    "repair-1",
                    new NodeId("worker-a"),
                    NOW,
                    Duration.ofSeconds(30)
            );

            assertThat(lease).isPresent();
            assertThat(runtime.repairLeaseStore().currentLease("repair-1")).contains(lease.orElseThrow());
        }
    }

    @Test
    void replaysHintsThroughBackgroundRuntimeLoop() throws InterruptedException {
        try (KvNodeRuntime nodeB = KvNodeRuntime.start(KvNodeConfig.fromProperties(baseProperties("node-b", tempDir.resolve("node-b"))));
             KvNodeRuntime nodeA = KvNodeRuntime.start(KvNodeConfig.fromProperties(replayRuntimeProperties(
                     "node-a",
                     tempDir.resolve("node-a"),
                     nodeB.localNode().port()
             )))) {
            Key key = Key.utf8("user:hinted");
            MutationRecord mutation = put(key, "alice-from-hint", 3);
            nodeA.hintStore().append(new HintRecord(
                    "hint-1",
                    nodeB.localNode().nodeId(),
                    mutation,
                    NOW,
                    0,
                    Optional.empty()
            ));

            waitUntil(Duration.ofSeconds(3), () -> nodeA.hintStore().loadAll().isEmpty());

            assertThat(nodeB.storage().get(key)).contains(StoredRecord.from(mutation));
        }
    }

    @Test
    void replayHintsNowReschedulesUnavailableTargets() {
        Properties properties = replayRuntimeProperties("node-a", tempDir.resolve("node-a-failed"), 65530);
        properties.setProperty(HintReplayConfig.ENABLED_PROPERTY, "false");

        try (KvNodeRuntime runtime = KvNodeRuntime.start(KvNodeConfig.fromProperties(properties))) {
            MutationRecord mutation = put(Key.utf8("user:retry"), "alice-retry", 4);
            runtime.hintStore().append(new HintRecord(
                    "hint-1",
                    new NodeId("node-b"),
                    mutation,
                    NOW,
                    0,
                    Optional.empty()
            ));

            HintReplaySummary summary = runtime.replayHintsNow();

            assertThat(summary).isEqualTo(new HintReplaySummary(1, 1, 0, 1, 0));
            assertThat(runtime.hintStore().loadAll())
                    .singleElement()
                    .satisfies(hint -> {
                        assertThat(hint.deliveryAttempts()).isEqualTo(1);
                        assertThat(hint.nextAttemptAt()).isPresent();
                    });
        }
    }

    private static Properties baseProperties(Path storagePath) {
        return baseProperties("node-a", storagePath);
    }

    private static Properties baseProperties(String nodeId, Path storagePath) {
        Properties properties = new Properties();
        properties.setProperty(KvNodeConfig.NODE_ID_PROPERTY, nodeId);
        properties.setProperty(KvNodeConfig.HOST_PROPERTY, "127.0.0.1");
        properties.setProperty(KvNodeConfig.PORT_PROPERTY, "0");
        properties.setProperty(KvNodeConfig.STORAGE_PATH_PROPERTY, storagePath.toString());
        return properties;
    }

    private static Properties replayRuntimeProperties(String nodeId, Path storagePath, int otherPort) {
        Properties properties = baseProperties(nodeId, storagePath);
        properties.setProperty(CoordinatorConfig.NODE_COUNT_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.node-id", nodeId);
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "0.self", "true");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.node-id", "node-b");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.host", "127.0.0.1");
        properties.setProperty(CoordinatorConfig.NODE_PREFIX + "1.port", Integer.toString(otherPort));
        properties.setProperty(CoordinatorConfig.REPLICATION_FACTOR_PROPERTY, "2");
        properties.setProperty(CoordinatorConfig.VNODE_COUNT_PROPERTY, "16");
        properties.setProperty(HintReplayConfig.INTERVAL_PROPERTY, "PT0.05S");
        properties.setProperty(HintReplayConfig.INITIAL_BACKOFF_PROPERTY, "PT0.05S");
        properties.setProperty(HintReplayConfig.MAX_BACKOFF_PROPERTY, "PT1S");
        return properties;
    }

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25L);
        }
        assertThat(condition.getAsBoolean()).isTrue();
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

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
