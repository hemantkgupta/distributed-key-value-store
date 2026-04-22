package com.hkg.kv.repair;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

public final class HintReplayWorker {
    private final HintStore hintStore;
    private final HintDelivery hintDelivery;
    private final HintReplayPolicy replayPolicy;
    private final Clock clock;

    public HintReplayWorker(HintStore hintStore, HintDelivery hintDelivery) {
        this(hintStore, hintDelivery, HintReplayPolicy.defaults(), Clock.systemUTC());
    }

    public HintReplayWorker(
            HintStore hintStore,
            HintDelivery hintDelivery,
            HintReplayPolicy replayPolicy,
            Clock clock
    ) {
        if (hintStore == null) {
            throw new IllegalArgumentException("hint store must not be null");
        }
        if (hintDelivery == null) {
            throw new IllegalArgumentException("hint delivery must not be null");
        }
        if (replayPolicy == null) {
            throw new IllegalArgumentException("replay policy must not be null");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        this.hintStore = hintStore;
        this.hintDelivery = hintDelivery;
        this.replayPolicy = replayPolicy;
        this.clock = clock;
    }

    public HintReplaySummary replayDueHints() {
        Instant now = Instant.now(clock);
        List<HintRecord> hints = hintStore.loadAll();
        int attempted = 0;
        int delivered = 0;
        int failed = 0;
        int skipped = 0;

        for (HintRecord hint : hints) {
            if (!isDue(hint, now)) {
                skipped++;
                continue;
            }

            attempted++;
            if (deliver(hint)) {
                hintStore.remove(hint.hintId());
                delivered++;
            } else {
                hintStore.replace(hint.withFailedDelivery(replayPolicy.nextAttemptAt(hint, now)));
                failed++;
            }
        }

        return new HintReplaySummary(hints.size(), attempted, delivered, failed, skipped);
    }

    private boolean deliver(HintRecord hint) {
        try {
            return hintDelivery.deliver(hint);
        } catch (RuntimeException exception) {
            return false;
        }
    }

    private static boolean isDue(HintRecord hint, Instant now) {
        return hint.nextAttemptAt()
                .map(nextAttemptAt -> !now.isBefore(nextAttemptAt))
                .orElse(true);
    }
}
