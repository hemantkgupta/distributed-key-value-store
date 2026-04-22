package com.hkg.kv.repair;

import java.time.Duration;
import java.time.Instant;

public record HintReplayPolicy(Duration initialBackoff, Duration maxBackoff) {
    public HintReplayPolicy {
        if (initialBackoff == null || initialBackoff.isNegative() || initialBackoff.isZero()) {
            throw new IllegalArgumentException("initial backoff must be positive");
        }
        if (maxBackoff == null || maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException("max backoff must be at least initial backoff");
        }
    }

    public static HintReplayPolicy defaults() {
        return new HintReplayPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5));
    }

    public Instant nextAttemptAt(HintRecord hint, Instant now) {
        if (hint == null) {
            throw new IllegalArgumentException("hint must not be null");
        }
        if (now == null) {
            throw new IllegalArgumentException("now must not be null");
        }

        long multiplier = 1L << Math.min(hint.deliveryAttempts(), 30);
        Duration delay = initialBackoff.multipliedBy(multiplier);
        if (delay.compareTo(maxBackoff) > 0) {
            delay = maxBackoff;
        }
        return now.plus(delay);
    }
}
