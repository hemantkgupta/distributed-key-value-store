package com.hkg.kv.node;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.partitioning.KeyTokenHasher;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.replication.ReplicaReadResponse;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StorageEngine;
import com.hkg.kv.storage.StoredRecord;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class HttpReplicaTransportHandlers {
    private static final String POST = "POST";
    private static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";
    private static final String TEXT_CONTENT_TYPE = "text/plain; charset=utf-8";

    private final NodeId localNodeId;
    private final StorageEngine storage;

    public HttpReplicaTransportHandlers(NodeId localNodeId, StorageEngine storage) {
        if (localNodeId == null) {
            throw new IllegalArgumentException("local node id must not be null");
        }
        if (storage == null) {
            throw new IllegalArgumentException("storage engine must not be null");
        }
        this.localNodeId = localNodeId;
        this.storage = storage;
    }

    public HttpHandler replicaWriteHandler() {
        return exchange -> handle(exchange, this::handleReplicaWrite);
    }

    public HttpHandler replicaReadHandler() {
        return exchange -> handle(exchange, this::handleReplicaRead);
    }

    public HttpHandler merkleRangeStreamHandler() {
        return exchange -> handle(exchange, this::handleMerkleRangeStream);
    }

    private void handleReplicaWrite(HttpExchange exchange) throws IOException {
        MutationRecord mutation = HttpReplicaTransportCodec.decodeMutationRecord(exchange.getRequestBody().readAllBytes());
        storage.apply(mutation);
        sendBinary(
                exchange,
                200,
                HttpReplicaTransportCodec.encodeReplicaResponse(new ReplicaResponse(localNodeId, true, "ok"))
        );
    }

    private void handleReplicaRead(HttpExchange exchange) throws IOException {
        com.hkg.kv.common.Key key = HttpReplicaTransportCodec.decodeKey(exchange.getRequestBody().readAllBytes());
        Optional<StoredRecord> record = storage.get(key);
        ReplicaReadResponse response = ReplicaReadResponse.success(localNodeId, record, storage.digest(key));
        sendBinary(exchange, 200, HttpReplicaTransportCodec.encodeReplicaReadResponse(response));
    }

    private void handleMerkleRangeStream(HttpExchange exchange) throws IOException {
        TokenRange range = HttpReplicaTransportCodec.decodeTokenRange(exchange.getRequestBody().readAllBytes());
        ArrayList<StoredRecord> records = new ArrayList<>();
        for (StoredRecord record : storage.scanAll()) {
            if (range.contains(KeyTokenHasher.tokenFor(record.key()))) {
                records.add(record);
            }
        }
        sendBinary(exchange, 200, HttpReplicaTransportCodec.encodeStoredRecords(List.copyOf(records)));
    }

    private void handle(HttpExchange exchange, ExchangeHandler handler) throws IOException {
        try {
            if (!POST.equals(exchange.getRequestMethod())) {
                sendText(exchange, 405, "method not allowed");
                return;
            }
            handler.handle(exchange);
        } catch (IllegalArgumentException exception) {
            sendText(exchange, 400, exception.getMessage());
        } catch (RuntimeException exception) {
            sendText(exchange, 500, describe(exception));
        } finally {
            exchange.close();
        }
    }

    private static void sendBinary(HttpExchange exchange, int statusCode, byte[] responseBody) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", OCTET_STREAM_CONTENT_TYPE);
        exchange.sendResponseHeaders(statusCode, responseBody.length);
        exchange.getResponseBody().write(responseBody);
    }

    private static void sendText(HttpExchange exchange, int statusCode, String responseText) throws IOException {
        byte[] responseBody = responseText.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", TEXT_CONTENT_TYPE);
        exchange.sendResponseHeaders(statusCode, responseBody.length);
        exchange.getResponseBody().write(responseBody);
    }

    private static String describe(RuntimeException exception) {
        String message = exception.getMessage();
        if (message == null || message.isBlank()) {
            return exception.getClass().getSimpleName();
        }
        return exception.getClass().getSimpleName() + ": " + message;
    }

    @FunctionalInterface
    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
