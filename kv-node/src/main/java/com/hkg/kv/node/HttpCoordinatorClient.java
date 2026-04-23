package com.hkg.kv.node;

import com.hkg.kv.partitioning.ClusterNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class HttpCoordinatorClient {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
    private static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";

    private final HttpClient httpClient;
    private final Duration requestTimeout;

    public HttpCoordinatorClient() {
        this(HttpClient.newHttpClient(), DEFAULT_TIMEOUT);
    }

    public HttpCoordinatorClient(HttpClient httpClient, Duration requestTimeout) {
        if (httpClient == null) {
            throw new IllegalArgumentException("http client must not be null");
        }
        if (requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("request timeout must be positive");
        }
        this.httpClient = httpClient;
        this.requestTimeout = requestTimeout;
    }

    public CoordinatorWriteResponse write(ClusterNode coordinator, CoordinatorWriteRequest request) {
        if (coordinator == null) {
            throw new IllegalArgumentException("coordinator node must not be null");
        }
        if (request == null) {
            throw new IllegalArgumentException("coordinator write request must not be null");
        }
        try {
            HttpResponse<byte[]> response = send(
                    coordinator,
                    HttpCoordinatorPaths.COORDINATOR_WRITE_PATH,
                    HttpCoordinatorCodec.encodeWriteRequest(request)
            );
            if (response.statusCode() != 200) {
                throw new IllegalStateException("http status " + response.statusCode());
            }
            return HttpCoordinatorCodec.decodeWriteResponse(response.body());
        } catch (IOException exception) {
            throw new IllegalStateException("failed to write through coordinator: " + describe(exception), exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("failed to write through coordinator: " + describe(exception), exception);
        }
    }

    public CoordinatorReadResponse read(ClusterNode coordinator, CoordinatorReadRequest request) {
        if (coordinator == null) {
            throw new IllegalArgumentException("coordinator node must not be null");
        }
        if (request == null) {
            throw new IllegalArgumentException("coordinator read request must not be null");
        }
        try {
            HttpResponse<byte[]> response = send(
                    coordinator,
                    HttpCoordinatorPaths.COORDINATOR_READ_PATH,
                    HttpCoordinatorCodec.encodeReadRequest(request)
            );
            if (response.statusCode() != 200) {
                throw new IllegalStateException("http status " + response.statusCode());
            }
            return HttpCoordinatorCodec.decodeReadResponse(response.body());
        } catch (IOException exception) {
            throw new IllegalStateException("failed to read through coordinator: " + describe(exception), exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("failed to read through coordinator: " + describe(exception), exception);
        }
    }

    private HttpResponse<byte[]> send(ClusterNode coordinator, String path, byte[] requestBody)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(uri(coordinator, path))
                .header("Content-Type", OCTET_STREAM_CONTENT_TYPE)
                .timeout(requestTimeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private static URI uri(ClusterNode coordinator, String path) {
        String host = coordinator.host().contains(":") ? "[" + coordinator.host() + "]" : coordinator.host();
        return URI.create("http://" + host + ":" + coordinator.port() + path);
    }

    private static String describe(Exception exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return exception.getClass().getSimpleName() + ": " + message;
    }
}
