package com.hkg.kv.node;

public enum RepairLeaseStoreBackend {
    IN_MEMORY,
    JDBC;

    static RepairLeaseStoreBackend fromConfigValue(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return IN_MEMORY;
        }
        String normalized = rawValue.trim().replace('-', '_').toUpperCase();
        for (RepairLeaseStoreBackend backend : values()) {
            if (backend.name().equals(normalized)) {
                return backend;
            }
        }
        throw new IllegalArgumentException("unsupported repair lease backend: " + rawValue);
    }
}
