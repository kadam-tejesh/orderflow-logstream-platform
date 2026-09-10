package com.orderflow.loginestion.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogForwardingClientTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void shouldForwardLogWhenSearchApiReturns201() throws Exception {

        server = createServer(201, "Indexed successfully");

        LogForwardingClient client =
                new LogForwardingClient(baseUrl());

        assertDoesNotThrow(() ->
                client.forwardLog(
                        "1700000000000",
                        "INFO",
                        "order-service",
                        "Order created"
                )
        );
    }

    @Test
    void shouldThrowExceptionWhenSearchApiReturnsNon201() throws Exception {

        server = createServer(500, "Internal server error");

        LogForwardingClient client =
                new LogForwardingClient(baseUrl());

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> client.forwardLog(
                        "1700000000000",
                        "ERROR",
                        "payment-service",
                        "Payment failed"
                )
        );

        assertTrue(exception.getMessage().contains("status 500"));
    }

    @Test
    void shouldUseCurrentTimeWhenTimestampIsInvalid() throws Exception {

        server = createServer(201, "Indexed successfully");

        LogForwardingClient client =
                new LogForwardingClient(baseUrl());

        assertDoesNotThrow(() ->
                client.forwardLog(
                        "invalid-timestamp",
                        "WARN",
                        "inventory-service",
                        "Stock is low"
                )
        );
    }

    @Test
    void shouldEscapeQuotesAndBackslashesInMessage() throws Exception {

        server = createServer(201, "Indexed successfully");

        LogForwardingClient client =
                new LogForwardingClient(baseUrl());

        assertDoesNotThrow(() ->
                client.forwardLog(
                        "1700000000000",
                        "INFO",
                        "order-service",
                        "Order \"ABC\" was processed\\successfully"
                )
        );
    }

    @Test
    void shouldRejectEmptySearchApiUrl() {

        assertThrows(
                IllegalArgumentException.class,
                () -> new LogForwardingClient("")
        );
    }

    @Test
    void shouldForwardLogWithIso8601Timestamp() throws Exception {

        server = createServer(201, "Indexed successfully");

        LogForwardingClient client =
                new LogForwardingClient(baseUrl());

        assertDoesNotThrow(() ->
                client.forwardLog(
                        "2026-08-25T10:15:00",
                        "INFO",
                        "order-service",
                        "ISO timestamp test"
                )
        );
    }

    private HttpServer createServer(
            int statusCode,
            String responseBody) throws IOException {

        server = HttpServer.create(
                new InetSocketAddress(0),
                0
        );

        server.createContext(
                "/api/index",
                exchange -> handleRequest(
                        exchange,
                        statusCode,
                        responseBody
                )
        );

        server.start();

        return server;
    }

    private void handleRequest(
            HttpExchange exchange,
            int statusCode,
            String responseBody) throws IOException {

        exchange.getRequestBody().readAllBytes();

        byte[] response =
                responseBody.getBytes(StandardCharsets.UTF_8);

        exchange.sendResponseHeaders(
                statusCode,
                response.length
        );

        exchange.getResponseBody().write(response);
        exchange.getResponseBody().close();
    }

    private String baseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }
}