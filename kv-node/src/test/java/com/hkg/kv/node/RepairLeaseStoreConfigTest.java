package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.repair.JdbcMerkleRepairLeaseStore;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class RepairLeaseStoreConfigTest {
    @Test
    void defaultsToInMemoryBackend() {
        RepairLeaseStoreConfig config = RepairLeaseStoreConfig.fromProperties(new Properties());

        assertThat(config.backend()).isEqualTo(RepairLeaseStoreBackend.IN_MEMORY);
        assertThat(config.jdbcTableName()).isEqualTo(JdbcMerkleRepairLeaseStore.DEFAULT_TABLE_NAME);
        assertThat(config.initializeSchema()).isFalse();
    }

    @Test
    void parsesJdbcBackendProperties() {
        Properties properties = new Properties();
        properties.setProperty(RepairLeaseStoreConfig.BACKEND_PROPERTY, "jdbc");
        properties.setProperty(RepairLeaseStoreConfig.JDBC_URL_PROPERTY, "jdbc:postgresql://localhost:5432/kv");
        properties.setProperty(RepairLeaseStoreConfig.JDBC_USERNAME_PROPERTY, "kv_user");
        properties.setProperty(RepairLeaseStoreConfig.JDBC_PASSWORD_PROPERTY, "secret");
        properties.setProperty(RepairLeaseStoreConfig.JDBC_TABLE_PROPERTY, "repair_leases");
        properties.setProperty(RepairLeaseStoreConfig.JDBC_INITIALIZE_SCHEMA_PROPERTY, "false");

        RepairLeaseStoreConfig config = RepairLeaseStoreConfig.fromProperties(properties);

        assertThat(config.backend()).isEqualTo(RepairLeaseStoreBackend.JDBC);
        assertThat(config.jdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5432/kv");
        assertThat(config.jdbcUsername()).isEqualTo("kv_user");
        assertThat(config.jdbcPassword()).isEqualTo("secret");
        assertThat(config.jdbcTableName()).isEqualTo("repair_leases");
        assertThat(config.initializeSchema()).isFalse();
    }

    @Test
    void rejectsJdbcBackendWithoutUrl() {
        Properties properties = new Properties();
        properties.setProperty(RepairLeaseStoreConfig.BACKEND_PROPERTY, "jdbc");

        assertThatThrownBy(() -> RepairLeaseStoreConfig.fromProperties(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jdbc url is required");
    }

    @Test
    void rejectsUnknownBackend() {
        Properties properties = new Properties();
        properties.setProperty(RepairLeaseStoreConfig.BACKEND_PROPERTY, "zookeeper");

        assertThatThrownBy(() -> RepairLeaseStoreConfig.fromProperties(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported repair lease backend");
    }
}
