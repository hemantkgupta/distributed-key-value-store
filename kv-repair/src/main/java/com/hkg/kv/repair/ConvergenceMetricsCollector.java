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
        return snapshot(null, null, null);
    }

    public ConvergenceMetricsSnapshot snapshot(HintReplaySummary hintReplaySummary, ReadRepairResult readRepairResult) {
        return snapshot(hintReplaySummary, readRepairResult, null);
    }

    public ConvergenceMetricsSnapshot snapshot(
            HintReplaySummary hintReplaySummary,
            ReadRepairResult readRepairResult,
            MerkleRepairResult merkleRepairResult
    ) {
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
                readRepairResult == null ? 0 : readRepairResult.missingTargets(),
                merkleRepairResult == null ? 0 : merkleRepairResult.differingRanges(),
                merkleRepairResult == null ? 0 : merkleRepairResult.scannedLeftRecords(),
                merkleRepairResult == null ? 0 : merkleRepairResult.scannedRightRecords(),
                merkleRepairResult == null ? 0 : merkleRepairResult.appliedToLeft(),
                merkleRepairResult == null ? 0 : merkleRepairResult.appliedToRight(),
                merkleRepairResult == null ? 0 : merkleRepairResult.failedWrites(),
                merkleRepairResult == null ? 0 : merkleRepairResult.alreadyConvergedKeys(),
                merkleRepairResult == null ? 0 : merkleRepairResult.skippedRanges(),
                merkleRepairResult != null && merkleRepairResult.stoppedByBudget() ? 1 : 0
        );
    }
}
