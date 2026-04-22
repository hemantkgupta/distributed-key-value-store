package com.hkg.kv.repair;

import java.util.Map;

public record ConvergenceMetric(String name, long value, Map<String, String> attributes) {
    public ConvergenceMetric(String name, long value) {
        this(name, value, Map.of());
    }

    public ConvergenceMetric {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("metric name must not be blank");
        }
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}
