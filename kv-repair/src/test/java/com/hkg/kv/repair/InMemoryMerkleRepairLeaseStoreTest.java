package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.kv.common.NodeId;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class InMemoryMerkleRepairLeaseStoreTest {
    private static final NodeId WORKER_A = new NodeId("worker-a");
    private static final NodeId WORKER_B = new NodeId("worker-b");
    private static final Instant NOW = Instant.parse("2026-04-22T00:00:00Z");
    private static final Duration TTL = Duration.ofSeconds(5);

    @Test
    void acquiresLeaseForUnownedTask() {
        InMemoryMerkleRepairLeaseStore store = new InMemoryMerkleRepairLeaseStore();

        Optional<MerkleRepairLease> lease = store.tryAcquire("task-a", WORKER_A, NOW, TTL);

        assertThat(lease).isPresent();
        assertThat(lease.orElseThrow().taskId()).isEqualTo("task-a");
        assertThat(lease.orElseThrow().owner()).isEqualTo(WORKER_A);
        assertThat(lease.orElseThrow().fencingToken()).isEqualTo(1);
        assertThat(lease.orElseThrow().expiresAt()).isEqualTo(NOW.plus(TTL));
    }

    @Test
    void rejectsAcquireWhenExistingLeaseIsStillValid() {
        InMemoryMerkleRepairLeaseStore store = new InMemoryMerkleRepairLeaseStore();
        MerkleRepairLease first = store.tryAcquire("task-a", WORKER_A, NOW, TTL).orElseThrow();

        Optional<MerkleRepairLease> second = store.tryAcquire("task-a", WORKER_B, NOW.plusSeconds(1), TTL);

        assertThat(second).isEmpty();
        assertThat(store.currentLease("task-a")).contains(first);
    }

    @Test
    void allowsAcquireAfterLeaseExpiryWithHigherFencingToken() {
        InMemoryMerkleRepairLeaseStore store = new InMemoryMerkleRepairLeaseStore();
        MerkleRepairLease first = store.tryAcquire("task-a", WORKER_A, NOW, TTL).orElseThrow();

        MerkleRepairLease second = store.tryAcquire("task-a", WORKER_B, NOW.plusSeconds(5), TTL).orElseThrow();

        assertThat(first.isExpiredAt(NOW.plusSeconds(5))).isTrue();
        assertThat(second.owner()).isEqualTo(WORKER_B);
        assertThat(second.fencingToken()).isGreaterThan(first.fencingToken());
    }

    @Test
    void releasesOnlyMatchingLease() {
        InMemoryMerkleRepairLeaseStore store = new InMemoryMerkleRepairLeaseStore();
        MerkleRepairLease first = store.tryAcquire("task-a", WORKER_A, NOW, TTL).orElseThrow();
        MerkleRepairLease stale = new MerkleRepairLease(
                first.taskId(),
                first.owner(),
                first.fencingToken() + 1,
                first.acquiredAt(),
                first.expiresAt()
        );

        assertThat(store.release(stale)).isFalse();
        assertThat(store.currentLease("task-a")).contains(first);
        assertThat(store.release(first)).isTrue();
        assertThat(store.currentLease("task-a")).isEmpty();
    }
}
