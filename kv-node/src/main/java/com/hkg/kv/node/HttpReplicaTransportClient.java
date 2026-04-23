package com.hkg.kv.node;

import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.repair.MerkleRangeStreamer;
import com.hkg.kv.replication.ReplicaReadResponse;
import com.hkg.kv.replication.ReplicaReader;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.replication.ReplicaWriter;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StoredRecord;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public final class HttpReplicaTransportClient implements ReplicaWriter, ReplicaReader, MerkleRangeStreamer {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
    private static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";

    private final HttpClient httpClient;
    private final Duration requestTimeout;

    public HttpReplicaTransportClient() {
        this(HttpClient.newHttpClient(), DEFAULT_TIMEOUT);
    }

    public HttpReplicaTransportClient(HttpClient httpClient, Duration requestTimeout) {
        if (httpClient == null) {
            throw new IllegalArgumentException("http client must not be null");
        }
        if (requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("request timeout must be positive");
        }
        this.httpClient = httpClient;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public ReplicaResponse write(ClusterNode replica, MutationRecord mutation) {
        if (replica == null) {
            throw new IllegalArgumentException("replica must not be null");
        }
        if (mutation == null) {
            throw new IllegalArgumentException("mutation record must not be null");
        }
        try {
            HttpResponse<byte[]> response = send(
                    replica,
                    HttpReplicaTransportPaths.REPLICA_WRITE_PATH,
                    HttpReplicaTransportCodec.encodeMutationRecord(mutation)
            );
            if (response.statusCode() != 200) {
                return new ReplicaResponse(replica.nodeId(), false, "http status " + response.statusCode());
            }
            return HttpReplicaTransportCodec.decodeReplicaResponse(response.body());
        } catch (IOException exception) {
            return new ReplicaResponse(replica.nodeId(), false, describe(exception));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return new ReplicaResponse(replica.nodeId(), false, describe(exception));
        } catch (RuntimeException exception) {
            return new ReplicaResponse(replica.nodeId(), false, describe(exception));
        }
    }

    @Override
    public ReplicaReadResponse read(ClusterNode replica, Key key) {
        if (replica == null) {
            throw new IllegalArgumentException("replica must not be null");
        }
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        try {
            HttpResponse<byte[]> response = send(
                    replica,
                    HttpReplicaTransportPaths.REPLICA_READ_PATH,
                    HttpReplicaTransportCodec.encodeKey(key)
            );
            if (response.statusCode() != 200) {
                return ReplicaReadResponse.failure(replica.nodeId(), "http status " + response.statusCode());
            }
            return HttpReplicaTransportCodec.decodeReplicaReadResponse(response.body());
        } catch (IOException exception) {
            return ReplicaReadResponse.failure(replica.nodeId(), describe(exception));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return ReplicaReadResponse.failure(replica.nodeId(), describe(exception));
        } catch (RuntimeException exception) {
            return ReplicaReadResponse.failure(replica.nodeId(), describe(exception));
        }
    }

    @Override
    public List<StoredRecord> stream(ClusterNode replica, TokenRange range) {
        if (replica == null) {
            throw new IllegalArgumentException("replica must not be null");
        }
        if (range == null) {
            throw new IllegalArgumentException("token range must not be null");
        }
        try {
            HttpResponse<byte[]> response = send(
                    replica,
                    HttpReplicaTransportPaths.MERKLE_RANGE_STREAM_PATH,
                    HttpReplicaTransportCodec.encodeTokenRange(range)
            );
            if (response.statusCode() != 200) {
                throw new IllegalStateException("http status " + response.statusCode());
            }
            return HttpReplicaTransportCodec.decodeStoredRecords(response.body());
        } catch (IOException exception) {
            throw new IllegalStateException("failed to stream Merkle range: " + describe(exception), exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("failed to stream Merkle range: " + describe(exception), exception);
        }
    }

    private HttpResponse<byte[]> send(ClusterNode replica, String path, byte[] requestBody)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(uri(replica, path))
                .header("Content-Type", OCTET_STREAM_CONTENT_TYPE)
                .timeout(requestTimeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private static URI uri(ClusterNode replica, String path) {
        String host = replica.host().contains(":") ? "[" + replica.host() + "]" : replica.host();
        return URI.create("http://" + host + ":" + replica.port() + path);
    }

    private static String describe(Exception exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return exception.getClass().getSimpleName() + ": " + message;
    }
}
