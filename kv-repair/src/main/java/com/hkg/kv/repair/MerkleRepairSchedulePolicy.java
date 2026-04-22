package com.hkg.kv.repair;

import java.time.Duration;
import java.time.Instant;

public record MerkleRepairSchedulePolicy(
        Duration cleanInterval,
        Duration initialIncompleteBackoff,
        Duration maxIncompleteBackoff
) {
    public MerkleRepairSchedulePolicy {
        if (cleanInterval == null || cleanInterval.isNegative() || cleanInterval.isZero()) {
            throw new IllegalArgumentException("clean interval must be positive");
        }
        if (initialIncompleteBackoff == null
                || initialIncompleteBackoff.isNegative()
                || initialIncompleteBackoff.isZero()) {
            throw new IllegalArgumentException("initial incomplete backoff must be positive");
        }
        if (maxIncompleteBackoff == null || maxIncompleteBackoff.compareTo(initialIncompleteBackoff) < 0) {
            throw new IllegalArgumentException("max incomplete backoff must be at least initial backoff");
        }
    }

    public static MerkleRepairSchedulePolicy defaults() {
        return new MerkleRepairSchedulePolicy(Duration.ofMinutes(10), Duration.ofSeconds(30), Duration.ofMinutes(10));
    }

    public Instant nextCleanRunAt(Instant now) {
        if (now == null) {
            throw new IllegalArgumentException("now must not be null");
        }
        return now.plus(cleanInterval);
    }

    public Instant nextIncompleteRunAt(int consecutiveIncompleteRuns, Instant now) {
        if (consecutiveIncompleteRuns < 1) {
            throw new IllegalArgumentException("consecutive incomplete runs must be positive");
        }
        if (now == null) {
            throw new IllegalArgumentException("now must not be null");
        }

        long multiplier = 1L << Math.min(consecutiveIncompleteRuns - 1, 30);
        Duration delay = initialIncompleteBackoff.multipliedBy(multiplier);
        if (delay.compareTo(maxIncompleteBackoff) > 0) {
            delay = maxIncompleteBackoff;
        }
        return now.plus(delay);
    }
}
