package com.hkg.kv.node;

import com.hkg.kv.partitioning.ClusterNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public record CoordinatorConfig(
        List<CoordinatorReplicaConfig> replicas,
        String localDatacenter,
        int maxAttempts,
        boolean readRepairEnabled
) {
    public static final String REPLICA_COUNT_PROPERTY = "kv.node.coordinator.replica-count";
    public static final String REPLICA_PREFIX = "kv.node.coordinator.replicas.";
    public static final String LOCAL_DATACENTER_PROPERTY = "kv.node.coordinator.local-datacenter";
    public static final String MAX_ATTEMPTS_PROPERTY = "kv.node.coordinator.max-attempts";
    public static final String READ_REPAIR_ENABLED_PROPERTY = "kv.node.coordinator.read-repair-enabled";

    public CoordinatorConfig {
        if (replicas == null) {
            throw new IllegalArgumentException("coordinator replicas must not be null");
        }
        replicas = List.copyOf(replicas);
        for (CoordinatorReplicaConfig replica : replicas) {
            if (replica == null) {
                throw new IllegalArgumentException("coordinator replicas must not contain null entries");
            }
        }
        localDatacenter = normalize(localDatacenter);
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("coordinator max attempts must be positive");
        }
    }

    public static CoordinatorConfig localOnly() {
        return new CoordinatorConfig(List.of(), null, 1, true);
    }

    public static CoordinatorConfig fromProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        int replicaCount = parseNonNegativeInt(properties.getProperty(REPLICA_COUNT_PROPERTY), 0, REPLICA_COUNT_PROPERTY);
        ArrayList<CoordinatorReplicaConfig> replicas = new ArrayList<>(replicaCount);
        for (int index = 0; index < replicaCount; index++) {
            String prefix = REPLICA_PREFIX + index + ".";
            replicas.add(new CoordinatorReplicaConfig(
                    new com.hkg.kv.common.NodeId(requiredProperty(properties, prefix + "node-id")),
                    Boolean.parseBoolean(properties.getProperty(prefix + "self", "false")),
                    properties.getProperty(prefix + "host"),
                    parseOptionalPort(properties.getProperty(prefix + "port"), prefix + "port"),
                    properties.getProperty(prefix + "datacenter")
            ));
        }
        return new CoordinatorConfig(
                List.copyOf(replicas),
                properties.getProperty(LOCAL_DATACENTER_PROPERTY),
                parseNonNegativeInt(properties.getProperty(MAX_ATTEMPTS_PROPERTY), 1, MAX_ATTEMPTS_PROPERTY),
                Boolean.parseBoolean(properties.getProperty(READ_REPAIR_ENABLED_PROPERTY, "true"))
        );
    }

    public List<ClusterNode> resolveReplicas(ClusterNode localNode) {
        if (localNode == null) {
            throw new IllegalArgumentException("local node must not be null");
        }
        if (replicas.isEmpty()) {
            return List.of(new CoordinatorReplicaConfig(localNode.nodeId(), true, null, 0, localDatacenter)
                    .resolve(localNode, localDatacenter));
        }
        ArrayList<ClusterNode> resolved = new ArrayList<>(replicas.size());
        for (CoordinatorReplicaConfig replica : replicas) {
            resolved.add(replica.resolve(localNode, localDatacenter));
        }
        return List.copyOf(resolved);
    }

    private static String requiredProperty(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(propertyName + " is required");
        }
        return value;
    }

    private static int parseNonNegativeInt(String value, int defaultValue, String propertyName) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(propertyName + " must not be negative");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("invalid integer for " + propertyName + ": " + value, exception);
        }
    }

    private static Integer parseOptionalPort(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("invalid integer for " + propertyName + ": " + value, exception);
        }
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
