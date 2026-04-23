package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.NodeId;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

class JdbcMerkleRepairLeaseStoreTest {
    private static final AtomicInteger DATABASE_ID = new AtomicInteger();
    private static final NodeId WORKER_A = new NodeId("worker-a");
    private static final NodeId WORKER_B = new NodeId("worker-b");
    private static final Instant NOW = Instant.parse("2026-04-23T00:00:00Z");
    private static final Duration TTL = Duration.ofSeconds(5);

    @Test
    void initializesSchemaAndAcquiresLease() {
        JdbcMerkleRepairLeaseStore store = newStore();

        Optional<MerkleRepairLease> lease = store.tryAcquire("task-a", WORKER_A, NOW, TTL);

        assertThat(lease).isPresent();
        assertThat(lease.orElseThrow().owner()).isEqualTo(WORKER_A);
        assertThat(lease.orElseThrow().fencingToken()).isEqualTo(1);
        assertThat(lease.orElseThrow().expiresAt()).isEqualTo(NOW.plus(TTL));
        assertThat(store.currentLease("task-a")).contains(lease.orElseThrow());
    }

    @Test
    void rejectsAcquireWhenAnotherWorkerHoldsValidLeaseAcrossStoreInstances() {
        JdbcDataSource dataSource = dataSource();
        JdbcMerkleRepairLeaseStore firstStore = newStore(dataSource);
        JdbcMerkleRepairLeaseStore secondStore = new JdbcMerkleRepairLeaseStore(dataSource);
        MerkleRepairLease first = firstStore.tryAcquire("task-a", WORKER_A, NOW, TTL).orElseThrow();

        Optional<MerkleRepairLease> second = secondStore.tryAcquire("task-a", WORKER_B, NOW.plusSeconds(1), TTL);

        assertThat(second).isEmpty();
        assertThat(secondStore.currentLease("task-a")).contains(first);
    }

    @Test
    void acquiresExpiredLeaseWithHigherFencingToken() {
        JdbcMerkleRepairLeaseStore store = newStore();
        MerkleRepairLease first = store.tryAcquire("task-a", WORKER_A, NOW, TTL).orElseThrow();

        MerkleRepairLease second = store.tryAcquire("task-a", WORKER_B, NOW.plusSeconds(5), TTL).orElseThrow();

        assertThat(first.isExpiredAt(NOW.plusSeconds(5))).isTrue();
        assertThat(second.owner()).isEqualTo(WORKER_B);
        assertThat(second.fencingToken()).isEqualTo(first.fencingToken() + 1);
        assertThat(store.currentLease("task-a")).contains(second);
    }

    @Test
    void releasesOnlyMatchingOwnerAndFencingToken() {
        JdbcMerkleRepairLeaseStore store = newStore();
        MerkleRepairLease first = store.tryAcquire("task-a", WORKER_A, NOW, TTL).orElseThrow();
        MerkleRepairLease stale = new MerkleRepairLease(
                first.taskId(),
                WORKER_B,
                first.fencingToken(),
                first.acquiredAt(),
                first.expiresAt()
        );

        assertThat(store.release(stale)).isFalse();
        assertThat(store.currentLease("task-a")).contains(first);
        assertThat(store.release(first)).isTrue();
        assertThat(store.currentLease("task-a")).isEmpty();
    }

    @Test
    void keepsFencingTokenMonotonicAfterRelease() {
        JdbcMerkleRepairLeaseStore store = newStore();
        MerkleRepairLease first = store.tryAcquire("task-a", WORKER_A, NOW, TTL).orElseThrow();
        assertThat(store.release(first)).isTrue();

        MerkleRepairLease second = store.tryAcquire("task-a", WORKER_A, NOW.plusSeconds(1), TTL).orElseThrow();

        assertThat(second.fencingToken()).isEqualTo(first.fencingToken() + 1);
        assertThat(store.currentLease("task-a")).contains(second);
    }

    private static JdbcMerkleRepairLeaseStore newStore() {
        return newStore(dataSource());
    }

    private static JdbcMerkleRepairLeaseStore newStore(JdbcDataSource dataSource) {
        JdbcMerkleRepairLeaseStore store = new JdbcMerkleRepairLeaseStore(dataSource);
        store.initializeSchema();
        return store;
    }

    private static JdbcDataSource dataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:lease-store-" + DATABASE_ID.incrementAndGet()
                + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
        return dataSource;
    }
}
