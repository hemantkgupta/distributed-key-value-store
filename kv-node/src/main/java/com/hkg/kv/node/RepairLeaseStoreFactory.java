package com.hkg.kv.node;

import com.hkg.kv.repair.InMemoryMerkleRepairLeaseStore;
import com.hkg.kv.repair.JdbcMerkleRepairLeaseStore;
import com.hkg.kv.repair.MerkleRepairLeaseStore;

public final class RepairLeaseStoreFactory {
    public MerkleRepairLeaseStore create(RepairLeaseStoreConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("repair lease store config must not be null");
        }
        return switch (config.backend()) {
            case IN_MEMORY -> new InMemoryMerkleRepairLeaseStore();
            case JDBC -> createJdbcStore(config);
        };
    }

    private static MerkleRepairLeaseStore createJdbcStore(RepairLeaseStoreConfig config) {
        JdbcMerkleRepairLeaseStore store = new JdbcMerkleRepairLeaseStore(
                new DriverManagerDataSource(config.jdbcUrl(), config.jdbcUsername(), config.jdbcPassword()),
                config.jdbcTableName()
        );
        if (config.initializeSchema()) {
            store.initializeSchema();
        }
        return store;
    }
}
