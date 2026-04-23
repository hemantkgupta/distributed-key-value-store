package com.hkg.kv.node;

import com.hkg.kv.repair.JdbcMerkleRepairLeaseStore;
import java.util.Properties;

public record RepairLeaseStoreConfig(
        RepairLeaseStoreBackend backend,
        String jdbcUrl,
        String jdbcUsername,
        String jdbcPassword,
        String jdbcTableName,
        boolean initializeSchema
) {
    public static final String BACKEND_PROPERTY = "kv.repair.lease.backend";
    public static final String JDBC_URL_PROPERTY = "kv.repair.lease.jdbc.url";
    public static final String JDBC_USERNAME_PROPERTY = "kv.repair.lease.jdbc.username";
    public static final String JDBC_PASSWORD_PROPERTY = "kv.repair.lease.jdbc.password";
    public static final String JDBC_TABLE_PROPERTY = "kv.repair.lease.jdbc.table";
    public static final String JDBC_INITIALIZE_SCHEMA_PROPERTY = "kv.repair.lease.jdbc.initialize-schema";

    public RepairLeaseStoreConfig {
        if (backend == null) {
            throw new IllegalArgumentException("repair lease backend must not be null");
        }
        jdbcUsername = normalize(jdbcUsername);
        jdbcPassword = jdbcPassword == null ? "" : jdbcPassword;
        if (jdbcTableName == null || jdbcTableName.isBlank()) {
            jdbcTableName = JdbcMerkleRepairLeaseStore.DEFAULT_TABLE_NAME;
        }
        if (backend == RepairLeaseStoreBackend.JDBC && (jdbcUrl == null || jdbcUrl.isBlank())) {
            throw new IllegalArgumentException("jdbc url is required for JDBC repair lease backend");
        }
    }

    public static RepairLeaseStoreConfig inMemory() {
        return new RepairLeaseStoreConfig(
                RepairLeaseStoreBackend.IN_MEMORY,
                null,
                null,
                null,
                JdbcMerkleRepairLeaseStore.DEFAULT_TABLE_NAME,
                false
        );
    }

    public static RepairLeaseStoreConfig jdbc(String jdbcUrl, String jdbcUsername, String jdbcPassword) {
        return new RepairLeaseStoreConfig(
                RepairLeaseStoreBackend.JDBC,
                jdbcUrl,
                jdbcUsername,
                jdbcPassword,
                JdbcMerkleRepairLeaseStore.DEFAULT_TABLE_NAME,
                true
        );
    }

    public static RepairLeaseStoreConfig fromProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        RepairLeaseStoreBackend backend = RepairLeaseStoreBackend.fromConfigValue(
                properties.getProperty(BACKEND_PROPERTY)
        );
        boolean initializeSchema = backend == RepairLeaseStoreBackend.JDBC
                && Boolean.parseBoolean(properties.getProperty(JDBC_INITIALIZE_SCHEMA_PROPERTY, "true"));
        return new RepairLeaseStoreConfig(
                backend,
                properties.getProperty(JDBC_URL_PROPERTY),
                properties.getProperty(JDBC_USERNAME_PROPERTY),
                properties.getProperty(JDBC_PASSWORD_PROPERTY),
                properties.getProperty(JDBC_TABLE_PROPERTY, JdbcMerkleRepairLeaseStore.DEFAULT_TABLE_NAME),
                initializeSchema
        );
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value;
    }
}
