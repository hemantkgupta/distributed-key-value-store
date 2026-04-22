package com.hkg.kv.replication;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.storage.StoredRecord;
import java.util.Arrays;
import java.util.Optional;

public final class ReplicaReadResponse {
    private final NodeId nodeId;
    private final boolean success;
    private final Optional<StoredRecord> record;
    private final byte[] digest;
    private final String detail;

    public ReplicaReadResponse(
            NodeId nodeId,
            boolean success,
            Optional<StoredRecord> record,
            byte[] digest,
            String detail
    ) {
        if (nodeId == null) {
            throw new IllegalArgumentException("node id must not be null");
        }
        this.nodeId = nodeId;
        this.success = success;
        this.record = record == null ? Optional.empty() : record;
        if (success && digest == null) {
            throw new IllegalArgumentException("successful read response must carry a digest");
        }
        this.digest = digest == null ? new byte[0] : digest.clone();
        this.detail = detail == null ? "" : detail;
    }

    public static ReplicaReadResponse success(
            NodeId nodeId,
            Optional<StoredRecord> record,
            byte[] digest
    ) {
        return new ReplicaReadResponse(nodeId, true, record, digest, "ok");
    }

    public static ReplicaReadResponse failure(NodeId nodeId, String detail) {
        return new ReplicaReadResponse(nodeId, false, Optional.empty(), null, detail);
    }

    public NodeId nodeId() {
        return nodeId;
    }

    public boolean success() {
        return success;
    }

    public Optional<StoredRecord> record() {
        return record;
    }

    public byte[] digest() {
        return digest.clone();
    }

    public String detail() {
        return detail;
    }

    public boolean sameDigest(ReplicaReadResponse other) {
        if (other == null) {
            throw new IllegalArgumentException("other response must not be null");
        }
        return Arrays.equals(digest, other.digest);
    }
}
