package com.hkg.kv.replication;

import com.hkg.kv.common.NodeId;

public record ReplicaResponse(NodeId nodeId, boolean success, String detail) {
    public ReplicaResponse {
        if (nodeId == null) {
            throw new IllegalArgumentException("node id must not be null");
        }
        detail = detail == null ? "" : detail;
    }
}
