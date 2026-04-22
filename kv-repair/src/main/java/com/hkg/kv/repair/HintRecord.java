package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.storage.MutationRecord;
import java.time.Instant;
import java.util.Optional;

public record HintRecord(
        String hintId,
        NodeId targetNodeId,
        MutationRecord mutation,
        Instant createdAt,
        int deliveryAttempts,
        Optional<Instant> nextAttemptAt
) {
    public HintRecord {
        if (hintId == null || hintId.isBlank()) {
            throw new IllegalArgumentException("hint id must not be blank");
        }
        if (targetNodeId == null) {
            throw new IllegalArgumentException("target node id must not be null");
        }
        if (mutation == null) {
            throw new IllegalArgumentException("mutation must not be null");
        }
        if (createdAt == null) {
            throw new IllegalArgumentException("created at must not be null");
        }
        if (deliveryAttempts < 0) {
            throw new IllegalArgumentException("delivery attempts must not be negative");
        }
        nextAttemptAt = nextAttemptAt == null ? Optional.empty() : nextAttemptAt;
    }

    public HintRecord withFailedDelivery(Instant nextAttemptAt) {
        if (nextAttemptAt == null) {
            throw new IllegalArgumentException("next attempt at must not be null");
        }
        return new HintRecord(
                hintId,
                targetNodeId,
                mutation,
                createdAt,
                deliveryAttempts + 1,
                Optional.of(nextAttemptAt)
        );
    }
}
