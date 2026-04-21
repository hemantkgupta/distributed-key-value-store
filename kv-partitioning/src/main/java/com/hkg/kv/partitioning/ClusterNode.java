package com.hkg.kv.partitioning;

import com.hkg.kv.common.NodeId;
import java.util.Map;

public record ClusterNode(NodeId nodeId, String host, int port, Map<String, String> labels) {
    public ClusterNode {
        if (nodeId == null) {
            throw new IllegalArgumentException("node id must not be null");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be valid");
        }
        labels = labels == null ? Map.of() : Map.copyOf(labels);
    }
}
