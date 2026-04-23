package com.hkg.kv.node;

import com.hkg.kv.partitioning.ConsistentHashReplicaPlacementPolicy;
import com.hkg.kv.partitioning.ClusterNode;
import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public record CoordinatorConfig(
        List<CoordinatorReplicaConfig> nodes,
        String localDatacenter,
        int maxAttempts,
        Duration requestBudget,
        boolean readRepairEnabled,
        Integer replicationFactor,
        int vnodeCount,
        long ringEpoch,
        boolean hintedHandoffEnabled,
        Path hintStorePath
) {
    public static final String NODE_COUNT_PROPERTY = "kv.node.coordinator.node-count";
    public static final String NODE_PREFIX = "kv.node.coordinator.nodes.";
    @Deprecated
    public static final String REPLICA_COUNT_PROPERTY = "kv.node.coordinator.replica-count";
    @Deprecated
    public static final String REPLICA_PREFIX = "kv.node.coordinator.replicas.";
    public static final String LOCAL_DATACENTER_PROPERTY = "kv.node.coordinator.local-datacenter";
    public static final String MAX_ATTEMPTS_PROPERTY = "kv.node.coordinator.max-attempts";
    public static final String REQUEST_BUDGET_PROPERTY = "kv.node.coordinator.request-budget";
    public static final String READ_REPAIR_ENABLED_PROPERTY = "kv.node.coordinator.read-repair-enabled";
    public static final String REPLICATION_FACTOR_PROPERTY = "kv.node.coordinator.replication-factor";
    public static final String VNODE_COUNT_PROPERTY = "kv.node.coordinator.vnode-count";
    public static final String RING_EPOCH_PROPERTY = "kv.node.coordinator.ring-epoch";
    public static final String HINTED_HANDOFF_ENABLED_PROPERTY = "kv.node.coordinator.hinted-handoff-enabled";
    public static final String HINT_STORE_PATH_PROPERTY = "kv.node.coordinator.hint-store-path";

    public static final int DEFAULT_VNODE_COUNT = 32;
    public static final long DEFAULT_RING_EPOCH = 1L;

    public CoordinatorConfig {
        if (nodes == null) {
            throw new IllegalArgumentException("coordinator nodes must not be null");
        }
        nodes = List.copyOf(nodes);
        for (CoordinatorReplicaConfig node : nodes) {
            if (node == null) {
                throw new IllegalArgumentException("coordinator nodes must not contain null entries");
            }
        }
        localDatacenter = normalize(localDatacenter);
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("coordinator max attempts must be positive");
        }
        if (requestBudget != null && (requestBudget.isZero() || requestBudget.isNegative())) {
            throw new IllegalArgumentException("coordinator request budget must be positive");
        }
        if (replicationFactor != null && replicationFactor < 1) {
            throw new IllegalArgumentException("coordinator replication factor must be positive");
        }
        if (vnodeCount < 1) {
            throw new IllegalArgumentException("coordinator vnode count must be positive");
        }
        if (ringEpoch < 1) {
            throw new IllegalArgumentException("coordinator ring epoch must be positive");
        }
    }

    public static CoordinatorConfig localOnly() {
        return new CoordinatorConfig(
                List.of(),
                null,
                1,
                null,
                true,
                null,
                DEFAULT_VNODE_COUNT,
                DEFAULT_RING_EPOCH,
                true,
                null
        );
    }

    public static CoordinatorConfig fromProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        int nodeCount = parseNonNegativeInt(
                property(properties, NODE_COUNT_PROPERTY, REPLICA_COUNT_PROPERTY),
                0,
                NODE_COUNT_PROPERTY
        );
        ArrayList<CoordinatorReplicaConfig> nodes = new ArrayList<>(nodeCount);
        for (int index = 0; index < nodeCount; index++) {
            String prefix = prefixForIndex(properties, index);
            nodes.add(new CoordinatorReplicaConfig(
                    new com.hkg.kv.common.NodeId(requiredProperty(properties, prefix + "node-id")),
                    Boolean.parseBoolean(properties.getProperty(prefix + "self", "false")),
                    properties.getProperty(prefix + "host"),
                    parseOptionalPort(properties.getProperty(prefix + "port"), prefix + "port"),
                    properties.getProperty(prefix + "datacenter")
            ));
        }
        return new CoordinatorConfig(
                List.copyOf(nodes),
                properties.getProperty(LOCAL_DATACENTER_PROPERTY),
                parseNonNegativeInt(properties.getProperty(MAX_ATTEMPTS_PROPERTY), 1, MAX_ATTEMPTS_PROPERTY),
                parseOptionalDuration(properties.getProperty(REQUEST_BUDGET_PROPERTY), REQUEST_BUDGET_PROPERTY),
                Boolean.parseBoolean(properties.getProperty(READ_REPAIR_ENABLED_PROPERTY, "true")),
                parseOptionalPositiveInt(properties.getProperty(REPLICATION_FACTOR_PROPERTY), REPLICATION_FACTOR_PROPERTY),
                parseNonNegativeInt(properties.getProperty(VNODE_COUNT_PROPERTY), DEFAULT_VNODE_COUNT, VNODE_COUNT_PROPERTY),
                parsePositiveLong(properties.getProperty(RING_EPOCH_PROPERTY), DEFAULT_RING_EPOCH, RING_EPOCH_PROPERTY),
                Boolean.parseBoolean(properties.getProperty(HINTED_HANDOFF_ENABLED_PROPERTY, "true")),
                parseOptionalPath(properties.getProperty(HINT_STORE_PATH_PROPERTY), HINT_STORE_PATH_PROPERTY)
        );
    }

    public boolean ringDrivenPlanningEnabled() {
        return replicationFactor != null;
    }

    public List<ClusterNode> resolveClusterNodes(ClusterNode localNode) {
        if (localNode == null) {
            throw new IllegalArgumentException("local node must not be null");
        }
        if (nodes.isEmpty()) {
            return List.of(new CoordinatorReplicaConfig(localNode.nodeId(), true, null, 0, localDatacenter)
                    .resolve(localNode, localDatacenter));
        }
        ArrayList<ClusterNode> resolved = new ArrayList<>(nodes.size());
        for (CoordinatorReplicaConfig node : nodes) {
            resolved.add(node.resolve(localNode, localDatacenter));
        }
        return List.copyOf(resolved);
    }

    public CoordinatorReplicaPlanner createReplicaPlanner(ClusterNode localNode) {
        List<ClusterNode> clusterNodes = resolveClusterNodes(localNode);
        if (!ringDrivenPlanningEnabled()) {
            return new StaticCoordinatorReplicaPlanner(clusterNodes);
        }
        if (replicationFactor > clusterNodes.size()) {
            throw new IllegalArgumentException("coordinator replication factor must not exceed configured cluster nodes");
        }
        return new RingCoordinatorReplicaPlanner(
                ConsistentHashReplicaPlacementPolicy.of(clusterNodes, vnodeCount, ringEpoch),
                replicationFactor
        );
    }

    public Path resolveHintStorePath(Path storagePath) {
        if (storagePath == null) {
            throw new IllegalArgumentException("storage path must not be null");
        }
        if (hintStorePath != null) {
            return hintStorePath;
        }
        return storagePath.resolve("coordinator-hints.log");
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

    private static Integer parseOptionalPositiveInt(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(propertyName + " must be positive");
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

    private static Duration parseOptionalDuration(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Duration.parse(value);
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException("invalid duration for " + propertyName + ": " + value, exception);
        }
    }

    private static long parsePositiveLong(String value, long defaultValue, String propertyName) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(propertyName + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("invalid long for " + propertyName + ": " + value, exception);
        }
    }

    private static Path parseOptionalPath(String value, String propertyName) {
        String normalized = normalize(value);
        if (normalized == null) {
            return null;
        }
        try {
            return Path.of(normalized);
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("invalid path for " + propertyName + ": " + value, exception);
        }
    }

    private static String property(Properties properties, String propertyName, String legacyPropertyName) {
        String current = properties.getProperty(propertyName);
        if (current != null && !current.isBlank()) {
            return current;
        }
        return properties.getProperty(legacyPropertyName);
    }

    private static String prefixForIndex(Properties properties, int index) {
        String currentPrefix = NODE_PREFIX + index + ".";
        if (properties.getProperty(currentPrefix + "node-id") != null) {
            return currentPrefix;
        }
        return REPLICA_PREFIX + index + ".";
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
