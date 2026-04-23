package com.hkg.kv.node;

import com.hkg.kv.repair.HintReplayPolicy;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Properties;

public record HintReplayConfig(
        boolean enabled,
        Duration interval,
        HintReplayPolicy policy
) {
    public static final String ENABLED_PROPERTY = "kv.node.hints.replay.enabled";
    public static final String INTERVAL_PROPERTY = "kv.node.hints.replay.interval";
    public static final String INITIAL_BACKOFF_PROPERTY = "kv.node.hints.replay.initial-backoff";
    public static final String MAX_BACKOFF_PROPERTY = "kv.node.hints.replay.max-backoff";

    public static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(1);

    public HintReplayConfig {
        if (interval == null || interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("hint replay interval must be positive");
        }
        if (policy == null) {
            throw new IllegalArgumentException("hint replay policy must not be null");
        }
    }

    public static HintReplayConfig defaults() {
        return new HintReplayConfig(true, DEFAULT_INTERVAL, HintReplayPolicy.defaults());
    }

    public static HintReplayConfig fromProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        HintReplayPolicy defaultPolicy = HintReplayPolicy.defaults();
        return new HintReplayConfig(
                Boolean.parseBoolean(properties.getProperty(ENABLED_PROPERTY, "true")),
                parseDuration(properties.getProperty(INTERVAL_PROPERTY), DEFAULT_INTERVAL, INTERVAL_PROPERTY),
                new HintReplayPolicy(
                        parseDuration(
                                properties.getProperty(INITIAL_BACKOFF_PROPERTY),
                                defaultPolicy.initialBackoff(),
                                INITIAL_BACKOFF_PROPERTY
                        ),
                        parseDuration(
                                properties.getProperty(MAX_BACKOFF_PROPERTY),
                                defaultPolicy.maxBackoff(),
                                MAX_BACKOFF_PROPERTY
                        )
                )
        );
    }

    private static Duration parseDuration(String value, Duration defaultValue, String propertyName) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Duration.parse(value);
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException("invalid duration for " + propertyName + ": " + value, exception);
        }
    }
}
