package com.orderflow.loginestion.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class LogForwardingClientTest {

    private HttpServer server;
    private AtomicReference<String> requestBody;

    @BeforeEach
    void setUp() {
        requestBody = new AtomicReference<>();
    }

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
                        "Order created successfully"
                )
        );

        String body = requestBody.get();

        assertNotNull(body);
        assertTrue(body.contains("\"level\":\"INFO\""));
        assertTrue(body.contains("\"service\":\"order-service\""));
        assertTrue(body.contains("\"timestamp\":1700000000000"));
        assertTrue(body.contains("\"message\":\"Order created successfully\""));
        assertTrue(body.contains("\"responseTime\":0"));
    }

    @Test
    void shouldForwardLogWithIso8601Timestamp() throws Exception {
        server = createServer(201, "Indexed successfully");

        LogForwardingClient client =
                new LogForwardingClient(baseUrl());

        String timestamp = "2026-08-25T10:15:00";

        long expectedTimestamp = LocalDateTime
                .parse(timestamp)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();

        assertDoesNotThrow(() ->
                client.forwardLog(
                        timestamp,
                        "INFO",
                        "order-service",
                        "ISO timestamp test"
                )
        );

        String body = requestBody.get();

        assertNotNull(body);
        assertTrue(
                body.contains("\"timestamp\":" + expectedTimestamp),
                "Request should contain the expected epoch timestamp"
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

        assertTrue(
                exception.getMessage().contains("Search API returned status 500")
        );
    }

    @Test
    void shouldUseCurrentTimeWhenTimestampIsInvalid() throws Exception {
        server = createServer(201, "Indexed successfully");

        LogForwardingClient client =
                new LogForwardingClient(baseUrl());

        long before = System.currentTimeMillis();

        assertDoesNotThrow(() ->
                client.forwardLog(
                        "not-a-valid-timestamp",
                        "WARN",
                        "order-service",
                        "Invalid timestamp test"
                )
        );

        long after = System.currentTimeMillis();

        String body = requestBody.get();

        assertNotNull(body);

        String timestampValue = extractTimestamp(body);

        long actualTimestamp = Long.parseLong(timestampValue);

        assertTrue(
                actualTimestamp >= before,
                "Fallback timestamp should not be earlier than the test start"
        );

        assertTrue(
                actualTimestamp <= after,
                "Fallback timestamp should not be later than the test end"
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
                        "ERROR",
                        "order-service",
                        "Message with \"quotes\" and \\backslash"
                )
        );

        String body = requestBody.get();

        assertNotNull(body);

        assertTrue(
                body.contains("\\\"quotes\\\""),
                "Quotes should be escaped in JSON"
        );

        assertTrue(
                body.contains("\\\\backslash"),
                "Backslashes should be escaped in JSON"
        );
    }

    @Test
    void shouldRejectEmptySearchApiUrl() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LogForwardingClient("")
        );
    }

    private HttpServer createServer(
            int statusCode,
            String responseBody) throws IOException {

        server = HttpServer.create(
                new InetSocketAddress(0),
                0
        );

        server.createContext("/api/index", exchange -> {
            try {
                requestBody.set(readRequestBody(exchange));

                byte[] response =
                        responseBody.getBytes(StandardCharsets.UTF_8);

                exchange.sendResponseHeaders(
                        statusCode,
                        response.length
                );

                exchange.getResponseBody().write(response);

            } finally {
                exchange.close();
            }
        });

        server.start();

        return server;
    }

    private String readRequestBody(HttpExchange exchange)
            throws IOException {

        try (InputStream inputStream =
                     exchange.getRequestBody()) {

            return new String(
                    inputStream.readAllBytes(),
                    StandardCharsets.UTF_8
            );
        }
    }

    private String baseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    private String extractTimestamp(String json) {

        String marker = "\"timestamp\":";

        int start = json.indexOf(marker);

        assertTrue(
                start >= 0,
                "Request should contain a timestamp field"
        );

        start += marker.length();

        int end = start;

        while (end < json.length()
                && Character.isDigit(json.charAt(end))) {
            end++;
        }

        return json.substring(start, end);
    }
}