package com.hkg.kv.repair;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class ConvergenceMetricsCollector {
    private final HintStore hintStore;

    public ConvergenceMetricsCollector(HintStore hintStore) {
        if (hintStore == null) {
            throw new IllegalArgumentException("hint store must not be null");
        }
        this.hintStore = hintStore;
    }

    public ConvergenceMetricsSnapshot snapshot() {
        return snapshot(null, null);
    }

    public ConvergenceMetricsSnapshot snapshot(HintReplaySummary hintReplaySummary, ReadRepairResult readRepairResult) {
        List<HintRecord> pendingHints = hintStore.loadAll();
        Optional<Instant> oldestHintCreatedAt = pendingHints.stream()
                .map(HintRecord::createdAt)
                .min(Comparator.naturalOrder());
        int maxHintDeliveryAttempts = pendingHints.stream()
                .mapToInt(HintRecord::deliveryAttempts)
                .max()
                .orElse(0);

        return new ConvergenceMetricsSnapshot(
                pendingHints.size(),
                oldestHintCreatedAt,
                maxHintDeliveryAttempts,
                hintReplaySummary == null ? 0 : hintReplaySummary.attempted(),
                hintReplaySummary == null ? 0 : hintReplaySummary.delivered(),
                hintReplaySummary == null ? 0 : hintReplaySummary.failed(),
                hintReplaySummary == null ? 0 : hintReplaySummary.skipped(),
                readRepairResult == null ? 0 : readRepairResult.attemptedRepairs(),
                readRepairResult == null ? 0 : readRepairResult.successfulRepairs(),
                readRepairResult == null ? 0 : readRepairResult.failedRepairs(),
                readRepairResult == null ? 0 : readRepairResult.missingTargets()
        );
    }
}
