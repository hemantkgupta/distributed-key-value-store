package com.hkg.kv.node;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public final class HttpCoordinatorHandlers {
    private static final String POST = "POST";
    private static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";
    private static final String TEXT_CONTENT_TYPE = "text/plain; charset=utf-8";

    private final CoordinatorService coordinatorService;

    public HttpCoordinatorHandlers(CoordinatorService coordinatorService) {
        if (coordinatorService == null) {
            throw new IllegalArgumentException("coordinator service must not be null");
        }
        this.coordinatorService = coordinatorService;
    }

    public HttpHandler coordinatorWriteHandler() {
        return exchange -> handle(exchange, this::handleWrite);
    }

    public HttpHandler coordinatorReadHandler() {
        return exchange -> handle(exchange, this::handleRead);
    }

    private void handleWrite(HttpExchange exchange) throws IOException {
        CoordinatorWriteRequest request = HttpCoordinatorCodec.decodeWriteRequest(exchange.getRequestBody().readAllBytes());
        CoordinatorWriteResponse response = coordinatorService.write(request);
        sendBinary(exchange, 200, HttpCoordinatorCodec.encodeWriteResponse(response));
    }

    private void handleRead(HttpExchange exchange) throws IOException {
        CoordinatorReadRequest request = HttpCoordinatorCodec.decodeReadRequest(exchange.getRequestBody().readAllBytes());
        CoordinatorReadResponse response = coordinatorService.read(request);
        sendBinary(exchange, 200, HttpCoordinatorCodec.encodeReadResponse(response));
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
