package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.repair.InMemoryMerkleRepairLeaseStore;
import com.hkg.kv.repair.JdbcMerkleRepairLeaseStore;
import com.hkg.kv.repair.MerkleRepairLease;
import com.hkg.kv.repair.MerkleRepairLeaseStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RepairLeaseStoreFactoryTest {
    private static final AtomicInteger DATABASE_ID = new AtomicInteger();
    private static final NodeId WORKER = new NodeId("worker-a");
    private static final Instant NOW = Instant.parse("2026-04-23T00:00:00Z");

    @Test
    void createsInMemoryLeaseStoreByDefault() {
        MerkleRepairLeaseStore store = new RepairLeaseStoreFactory().create(RepairLeaseStoreConfig.inMemory());

        assertThat(store).isInstanceOf(InMemoryMerkleRepairLeaseStore.class);
    }

    @Test
    void createsInitializedJdbcLeaseStore() {
        String jdbcUrl = "jdbc:h2:mem:node-lease-store-" + DATABASE_ID.incrementAndGet()
                + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        RepairLeaseStoreConfig config = new RepairLeaseStoreConfig(
                RepairLeaseStoreBackend.JDBC,
                jdbcUrl,
                null,
                null,
                JdbcMerkleRepairLeaseStore.DEFAULT_TABLE_NAME,
                true
        );

        MerkleRepairLeaseStore store = new RepairLeaseStoreFactory().create(config);
        Optional<MerkleRepairLease> lease = store.tryAcquire(
                "repair-a",
                WORKER,
                NOW,
                Duration.ofSeconds(30)
        );

        assertThat(store).isInstanceOf(JdbcMerkleRepairLeaseStore.class);
        assertThat(lease).isPresent();
        assertThat(store.currentLease("repair-a")).contains(lease.orElseThrow());
    }
}
