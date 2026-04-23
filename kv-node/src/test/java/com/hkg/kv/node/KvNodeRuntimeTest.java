package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
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

    private static Properties baseProperties(Path storagePath) {
        Properties properties = new Properties();
        properties.setProperty(KvNodeConfig.NODE_ID_PROPERTY, "node-a");
        properties.setProperty(KvNodeConfig.HOST_PROPERTY, "127.0.0.1");
        properties.setProperty(KvNodeConfig.PORT_PROPERTY, "0");
        properties.setProperty(KvNodeConfig.STORAGE_PATH_PROPERTY, storagePath.toString());
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
