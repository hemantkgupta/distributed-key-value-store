package com.hkg.kv.replication;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

public final class RequestBudget {
    private final boolean bounded;
    private final LongSupplier nanoTimeSupplier;
    private final long deadlineNanos;

    private RequestBudget(boolean bounded, LongSupplier nanoTimeSupplier, long deadlineNanos) {
        this.bounded = bounded;
        this.nanoTimeSupplier = Objects.requireNonNull(nanoTimeSupplier, "nano time supplier must not be null");
        this.deadlineNanos = deadlineNanos;
    }

    public static RequestBudget unbounded() {
        return new RequestBudget(false, System::nanoTime, Long.MAX_VALUE);
    }

    public static RequestBudget start(Duration budget) {
        return start(budget, System::nanoTime);
    }

    public static RequestBudget start(Duration budget, LongSupplier nanoTimeSupplier) {
        if (budget == null || budget.isZero() || budget.isNegative()) {
            throw new IllegalArgumentException("request budget must be positive");
        }
        Objects.requireNonNull(nanoTimeSupplier, "nano time supplier must not be null");
        long startNanos = nanoTimeSupplier.getAsLong();
        long budgetNanos = budget.toNanos();
        long deadlineNanos = Long.MAX_VALUE - budgetNanos < startNanos
                ? Long.MAX_VALUE
                : startNanos + budgetNanos;
        return new RequestBudget(true, nanoTimeSupplier, deadlineNanos);
    }

    public boolean bounded() {
        return bounded;
    }

    public boolean exhausted() {
        return bounded && nanoTimeSupplier.getAsLong() >= deadlineNanos;
    }
}
