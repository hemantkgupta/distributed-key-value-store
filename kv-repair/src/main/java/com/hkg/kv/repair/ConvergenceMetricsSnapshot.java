package com.hkg.kv.repair;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
        int missingReadRepairTargets,
        int differingMerkleRepairRanges,
        int scannedLeftMerkleRecords,
        int scannedRightMerkleRecords,
        int merkleRepairWritesToLeft,
        int merkleRepairWritesToRight,
        int failedMerkleRepairWrites,
        int alreadyConvergedMerkleKeys,
        int skippedMerkleRepairRanges,
        int merkleRepairBudgetStops
) {
    public ConvergenceMetricsSnapshot(
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
        this(
                pendingHints,
                oldestHintCreatedAt,
                maxHintDeliveryAttempts,
                attemptedHintReplays,
                deliveredHintReplays,
                failedHintReplays,
                skippedHintReplays,
                attemptedReadRepairs,
                successfulReadRepairs,
                failedReadRepairs,
                missingReadRepairTargets,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
        );
    }

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
                || missingReadRepairTargets < 0
                || differingMerkleRepairRanges < 0
                || scannedLeftMerkleRecords < 0
                || scannedRightMerkleRecords < 0
                || merkleRepairWritesToLeft < 0
                || merkleRepairWritesToRight < 0
                || failedMerkleRepairWrites < 0
                || alreadyConvergedMerkleKeys < 0
                || skippedMerkleRepairRanges < 0
                || merkleRepairBudgetStops < 0) {
            throw new IllegalArgumentException("metric counts must not be negative");
        }
        oldestHintCreatedAt = oldestHintCreatedAt == null ? Optional.empty() : oldestHintCreatedAt;
    }

    public int scannedMerkleRecords() {
        return scannedLeftMerkleRecords + scannedRightMerkleRecords;
    }

    public int successfulMerkleRepairWrites() {
        return merkleRepairWritesToLeft + merkleRepairWritesToRight;
    }

    public List<ConvergenceMetric> toMetrics() {
        ArrayList<ConvergenceMetric> metrics = new ArrayList<>();
        metrics.add(new ConvergenceMetric("kv.repair.hints.pending", pendingHints));
        oldestHintCreatedAt.ifPresent(instant -> metrics.add(new ConvergenceMetric(
                "kv.repair.hints.oldest_created_at_epoch_seconds",
                instant.getEpochSecond()
        )));
        metrics.add(new ConvergenceMetric("kv.repair.hints.max_delivery_attempts", maxHintDeliveryAttempts));
        metrics.add(new ConvergenceMetric("kv.repair.hints.replay.attempted", attemptedHintReplays));
        metrics.add(new ConvergenceMetric("kv.repair.hints.replay.delivered", deliveredHintReplays));
        metrics.add(new ConvergenceMetric("kv.repair.hints.replay.failed", failedHintReplays));
        metrics.add(new ConvergenceMetric("kv.repair.hints.replay.skipped", skippedHintReplays));
        metrics.add(new ConvergenceMetric("kv.repair.read_repair.attempted", attemptedReadRepairs));
        metrics.add(new ConvergenceMetric("kv.repair.read_repair.successful", successfulReadRepairs));
        metrics.add(new ConvergenceMetric("kv.repair.read_repair.failed", failedReadRepairs));
        metrics.add(new ConvergenceMetric("kv.repair.read_repair.missing_targets", missingReadRepairTargets));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.ranges.differing", differingMerkleRepairRanges));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.records.scanned", scannedMerkleRecords()));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.records.scanned_left", scannedLeftMerkleRecords));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.records.scanned_right", scannedRightMerkleRecords));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.writes.successful", successfulMerkleRepairWrites()));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.writes.to_left", merkleRepairWritesToLeft));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.writes.to_right", merkleRepairWritesToRight));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.writes.failed", failedMerkleRepairWrites));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.keys.already_converged", alreadyConvergedMerkleKeys));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.ranges.skipped", skippedMerkleRepairRanges));
        metrics.add(new ConvergenceMetric("kv.repair.merkle.budget_stops", merkleRepairBudgetStops));
        return List.copyOf(metrics);
    }
}
