package com.hkg.kv.replication;

import com.hkg.kv.common.NodeId;

public record ReplicaResponse(NodeId nodeId, boolean success, String detail, int attempts) {
    public ReplicaResponse(NodeId nodeId, boolean success, String detail) {
        this(nodeId, success, detail, 1);
    }

    public ReplicaResponse {
        if (nodeId == null) {
            throw new IllegalArgumentException("node id must not be null");
        }
        detail = detail == null ? "" : detail;
        if (attempts < 1) {
            throw new IllegalArgumentException("attempts must be positive");
        }
    }
}
