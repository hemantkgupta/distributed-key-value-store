package com.hkg.kv.node;

final class HttpReplicaTransportPaths {
    static final String REPLICA_WRITE_PATH = "/internal/replication/write";
    static final String REPLICA_READ_PATH = "/internal/replication/read";
    static final String MERKLE_RANGE_STREAM_PATH = "/internal/repair/stream-range";

    private HttpReplicaTransportPaths() {
    }
}
