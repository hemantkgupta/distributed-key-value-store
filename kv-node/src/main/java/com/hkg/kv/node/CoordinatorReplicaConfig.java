package com.hkg.kv.node;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.replication.ReplicationOptions;
import java.util.LinkedHashMap;
import java.util.Map;

public record CoordinatorReplicaConfig(
        NodeId nodeId,
        boolean self,
        String host,
        Integer port,
        String datacenter
) {
    public CoordinatorReplicaConfig {
        if (nodeId == null) {
            throw new IllegalArgumentException("coordinator replica node id must not be null");
        }
        host = normalize(host);
        datacenter = normalize(datacenter);
        if (self) {
            port = port == null ? 0 : port;
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("coordinator replica port must be between 0 and 65535");
            }
        } else {
            if (host == null) {
                throw new IllegalArgumentException("coordinator replica host is required for non-self entries");
            }
            if (port == null || port <= 0 || port > 65535) {
                throw new IllegalArgumentException("coordinator replica port must be valid for non-self entries");
            }
        }
    }

    public ClusterNode resolve(ClusterNode localNode, String defaultLocalDatacenter) {
        if (localNode == null) {
            throw new IllegalArgumentException("local node must not be null");
        }
        String resolvedDatacenter = datacenter != null ? datacenter : defaultLocalDatacenter;
        Map<String, String> labels = labels(resolvedDatacenter);
        if (self) {
            if (!nodeId.equals(localNode.nodeId())) {
                throw new IllegalArgumentException("self coordinator replica must match local node id");
            }
            return new ClusterNode(localNode.nodeId(), localNode.host(), localNode.port(), labels);
        }
        return new ClusterNode(nodeId, host, port == null ? 0 : port, labels);
    }

    private static Map<String, String> labels(String datacenter) {
        if (datacenter == null) {
            return Map.of();
        }
        LinkedHashMap<String, String> labels = new LinkedHashMap<>();
        labels.put(ReplicationOptions.DATACENTER_LABEL, datacenter);
        labels.put(ReplicationOptions.DC_LABEL, datacenter);
        return Map.copyOf(labels);
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
