package com.hkg.kv.node;

import com.hkg.kv.common.NodeId;
import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Properties;

public record KvNodeConfig(
        NodeId nodeId,
        String host,
        int port,
        Path storagePath,
        Duration requestTimeout,
        RepairLeaseStoreConfig repairLeaseStoreConfig,
        CoordinatorConfig coordinatorConfig
) {
    public static final String NODE_ID_PROPERTY = "kv.node.id";
    public static final String HOST_PROPERTY = "kv.node.host";
    public static final String PORT_PROPERTY = "kv.node.port";
    public static final String STORAGE_PATH_PROPERTY = "kv.node.storage.rocksdb.path";
    public static final String REQUEST_TIMEOUT_PROPERTY = "kv.node.http.request-timeout";

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 8080;
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(5);

    public KvNodeConfig {
        if (nodeId == null) {
            throw new IllegalArgumentException("node id must not be null");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (storagePath == null) {
            throw new IllegalArgumentException("storage path must not be null");
        }
        if (requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("request timeout must be positive");
        }
        if (repairLeaseStoreConfig == null) {
            throw new IllegalArgumentException("repair lease store config must not be null");
        }
        if (coordinatorConfig == null) {
            throw new IllegalArgumentException("coordinator config must not be null");
        }
    }

    public static KvNodeConfig fromProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        return new KvNodeConfig(
                new NodeId(requiredProperty(properties, NODE_ID_PROPERTY)),
                properties.getProperty(HOST_PROPERTY, DEFAULT_HOST),
                parsePort(properties.getProperty(PORT_PROPERTY)),
                Path.of(requiredProperty(properties, STORAGE_PATH_PROPERTY)),
                parseDuration(properties.getProperty(REQUEST_TIMEOUT_PROPERTY, DEFAULT_REQUEST_TIMEOUT.toString())),
                RepairLeaseStoreConfig.fromProperties(properties),
                CoordinatorConfig.fromProperties(properties)
        );
    }

    private static String requiredProperty(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(propertyName + " is required");
        }
        return value;
    }

    private static int parsePort(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_PORT;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("invalid port: " + value, exception);
        }
    }

    private static Duration parseDuration(String value) {
        try {
            return Duration.parse(value);
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException("invalid request timeout: " + value, exception);
        }
    }
}
