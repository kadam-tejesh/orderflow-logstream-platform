package com.orderflow.loginestion.service;

import com.orderflow.loginestion.grpc.LogRequest;
import com.orderflow.loginestion.grpc.LogResponse;
import com.sun.net.httpserver.HttpServer;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogIngestionServiceImplTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void shouldSendLogSuccessfullyWhenSearchApiReturns201()
            throws Exception {

        server = createServer(201, "Indexed successfully");

        LogIngestionServiceImpl service =
                new LogIngestionServiceImpl(baseUrl());

        LogRequest request = LogRequest.newBuilder()
                .setTimestamp("1700000000000")
                .setLevel("info")
                .setService("order-service")
                .setMessage("Order created")
                .build();

        TestResponseObserver observer =
                new TestResponseObserver();

        service.sendLog(request, observer);

        assertTrue(observer.completed);
        assertNotNull(observer.response);
        assertTrue(observer.response.getSuccess());

        assertEquals(
                "Log received and forwarded for indexing",
                observer.response.getMessage()
        );
    }

    @Test
    void shouldReturnErrorWhenSearchApiFails()
            throws Exception {

        server = createServer(500, "Internal server error");

        LogIngestionServiceImpl service =
                new LogIngestionServiceImpl(baseUrl());

        LogRequest request = LogRequest.newBuilder()
                .setTimestamp("1700000000000")
                .setLevel("error")
                .setService("payment-service")
                .setMessage("Payment failed")
                .build();

        TestResponseObserver observer =
                new TestResponseObserver();

        service.sendLog(request, observer);

        assertNotNull(observer.error);

        assertTrue(
                observer.error.getMessage().contains("status 500")
        );
    }

    @Test
    void shouldProcessMultipleLogsInStream()
            throws Exception {

        server = createServer(201, "Indexed successfully");

        LogIngestionServiceImpl service =
                new LogIngestionServiceImpl(baseUrl());

        TestResponseObserver observer =
                new TestResponseObserver();

        StreamObserver<LogRequest> requestObserver =
                service.streamLogs(observer);

        requestObserver.onNext(
                createRequest(
                        "1700000000000",
                        "info",
                        "order-service",
                        "Order created"
                )
        );

        requestObserver.onNext(
                createRequest(
                        "1700000001000",
                        "warn",
                        "inventory-service",
                        "Stock is low"
                )
        );

        requestObserver.onNext(
                createRequest(
                        "1700000002000",
                        "error",
                        "payment-service",
                        "Payment failed"
                )
        );

        requestObserver.onCompleted();

        assertTrue(observer.completed);
        assertNotNull(observer.response);
        assertTrue(observer.response.getSuccess());

        assertEquals(
                "3 logs received and forwarded for indexing",
                observer.response.getMessage()
        );
    }

    @Test
    void shouldNormalizeLogBeforeForwarding()
            throws Exception {

        AtomicReference<String> requestBody =
                new AtomicReference<>();

        server = HttpServer.create(
                new InetSocketAddress(0),
                0
        );

        server.createContext(
                "/api/index",
                exchange -> {

                    String body = new String(
                            exchange.getRequestBody().readAllBytes(),
                            StandardCharsets.UTF_8
                    );

                    requestBody.set(body);

                    byte[] response =
                            "Indexed successfully"
                                    .getBytes(StandardCharsets.UTF_8);

                    exchange.sendResponseHeaders(
                            201,
                            response.length
                    );

                    exchange.getResponseBody().write(response);
                    exchange.getResponseBody().close();
                }
        );

        server.start();

        LogIngestionServiceImpl service =
                new LogIngestionServiceImpl(baseUrl());

        LogRequest request = LogRequest.newBuilder()
                .setTimestamp(" 1700000000000 ")
                .setLevel("  info ")
                .setService("  order-service ")
                .setMessage("  Order created successfully  ")
                .build();

        TestResponseObserver observer =
                new TestResponseObserver();

        service.sendLog(request, observer);

        assertTrue(observer.completed);
        assertTrue(observer.response.getSuccess());

        assertNotNull(requestBody.get());

        assertTrue(
                requestBody.get().contains("\"level\":\"INFO\"")
        );

        assertTrue(
                requestBody.get().contains(
                        "\"service\":\"order-service\""
                )
        );

        assertTrue(
                requestBody.get().contains(
                        "\"timestamp\":1700000000000"
                )
        );

        assertTrue(
                requestBody.get().contains(
                        "\"message\":\"Order created successfully\""
                )
        );
    }

    private LogRequest createRequest(
            String timestamp,
            String level,
            String service,
            String message) {

        return LogRequest.newBuilder()
                .setTimestamp(timestamp)
                .setLevel(level)
                .setService(service)
                .setMessage(message)
                .build();
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
                exchange -> {

                    exchange.getRequestBody().readAllBytes();

                    byte[] response =
                            responseBody.getBytes(
                                    StandardCharsets.UTF_8
                            );

                    exchange.sendResponseHeaders(
                            statusCode,
                            response.length
                    );

                    exchange.getResponseBody().write(response);
                    exchange.getResponseBody().close();
                }
        );

        server.start();

        return server;
    }

    private String baseUrl() {
        return "http://localhost:"
                + server.getAddress().getPort();
    }

    private static class TestResponseObserver
            implements StreamObserver<LogResponse> {

        private LogResponse response;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(LogResponse response) {
            this.response = response;
        }

        @Override
        public void onError(Throwable throwable) {
            this.error = throwable;
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }
    }
}