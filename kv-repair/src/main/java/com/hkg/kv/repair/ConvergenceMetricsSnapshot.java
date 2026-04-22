package com.hkg.kv.repair;

import java.time.Instant;
import java.util.Optional;

public record ConvergenceMetricsSnapshot(
        int pendingHints,
        Optional<Instant> oldestHintCreatedAt,
        int maxHintDeliveryAttempts,
        int attemptedHintReplays,
        int deliveredHintReplays,
        int failedHintReplays,
        int skippedHintReplays,
        int attemptedReadRepairs,
        int successfulReadRepairs,
        int failedReadRepairs,
        int missingReadRepairTargets
) {
    public ConvergenceMetricsSnapshot {
        if (pendingHints < 0
                || maxHintDeliveryAttempts < 0
                || attemptedHintReplays < 0
                || deliveredHintReplays < 0
                || failedHintReplays < 0
                || skippedHintReplays < 0
                || attemptedReadRepairs < 0
                || successfulReadRepairs < 0
                || failedReadRepairs < 0
                || missingReadRepairTargets < 0) {
            throw new IllegalArgumentException("metric counts must not be negative");
        }
        oldestHintCreatedAt = oldestHintCreatedAt == null ? Optional.empty() : oldestHintCreatedAt;
    }
}
