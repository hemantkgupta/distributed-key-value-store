package com.hkg.kv.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KvNodeConfigTest {
    @TempDir
    Path tempDir;

    @Test
    void parsesRequiredPropertiesAndDefaults() {
        Properties properties = new Properties();
        properties.setProperty(KvNodeConfig.NODE_ID_PROPERTY, "node-a");
        properties.setProperty(KvNodeConfig.STORAGE_PATH_PROPERTY, tempDir.resolve("rocksdb").toString());

        KvNodeConfig config = KvNodeConfig.fromProperties(properties);

        assertThat(config.nodeId().value()).isEqualTo("node-a");
        assertThat(config.host()).isEqualTo(KvNodeConfig.DEFAULT_HOST);
        assertThat(config.port()).isEqualTo(KvNodeConfig.DEFAULT_PORT);
        assertThat(config.storagePath()).isEqualTo(tempDir.resolve("rocksdb"));
        assertThat(config.requestTimeout()).isEqualTo(KvNodeConfig.DEFAULT_REQUEST_TIMEOUT);
        assertThat(config.repairLeaseStoreConfig()).isEqualTo(RepairLeaseStoreConfig.inMemory());
    }

    @Test
    void parsesCustomHttpAndLeaseSettings() {
        Properties properties = new Properties();
        properties.setProperty(KvNodeConfig.NODE_ID_PROPERTY, "node-a");
        properties.setProperty(KvNodeConfig.HOST_PROPERTY, "127.0.0.1");
        properties.setProperty(KvNodeConfig.PORT_PROPERTY, "9042");
        properties.setProperty(KvNodeConfig.STORAGE_PATH_PROPERTY, tempDir.resolve("rocksdb").toString());
        properties.setProperty(KvNodeConfig.REQUEST_TIMEOUT_PROPERTY, "PT3S");
        properties.setProperty(RepairLeaseStoreConfig.BACKEND_PROPERTY, "jdbc");
        properties.setProperty(RepairLeaseStoreConfig.JDBC_URL_PROPERTY, "jdbc:h2:mem:kv-node;MODE=PostgreSQL");

        KvNodeConfig config = KvNodeConfig.fromProperties(properties);

        assertThat(config.port()).isEqualTo(9042);
        assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(config.repairLeaseStoreConfig().backend()).isEqualTo(RepairLeaseStoreBackend.JDBC);
        assertThat(config.repairLeaseStoreConfig().jdbcUrl()).isEqualTo("jdbc:h2:mem:kv-node;MODE=PostgreSQL");
    }

    @Test
    void rejectsMissingStoragePath() {
        Properties properties = new Properties();
        properties.setProperty(KvNodeConfig.NODE_ID_PROPERTY, "node-a");

        assertThatThrownBy(() -> KvNodeConfig.fromProperties(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(KvNodeConfig.STORAGE_PATH_PROPERTY);
    }

    @Test
    void rejectsInvalidRequestTimeout() {
        Properties properties = new Properties();
        properties.setProperty(KvNodeConfig.NODE_ID_PROPERTY, "node-a");
        properties.setProperty(KvNodeConfig.STORAGE_PATH_PROPERTY, tempDir.resolve("rocksdb").toString());
        properties.setProperty(KvNodeConfig.REQUEST_TIMEOUT_PROPERTY, "five-seconds");

        assertThatThrownBy(() -> KvNodeConfig.fromProperties(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid request timeout");
    }
}
