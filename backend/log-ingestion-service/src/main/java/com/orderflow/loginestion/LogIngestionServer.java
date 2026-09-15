package com.orderflow.loginestion;

import com.orderflow.loginestion.service.IngestionMetrics;
import com.orderflow.loginestion.service.LogIngestionServiceImpl;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class LogIngestionServer {

    private static final int GRPC_PORT = 9090;
    private static final int HEALTH_PORT = 9091;

    private static String loadSearchApiBaseUrl() {

        Properties properties = new Properties();

        try (InputStream input =
                     LogIngestionServer.class
                             .getClassLoader()
                             .getResourceAsStream("application.properties")) {

            if (input == null) {
                throw new RuntimeException(
                        "application.properties not found"
                );
            }

            properties.load(input);

            return properties.getProperty(
                    "search.api.base-url",
                    "http://localhost:8084"
            );

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load application.properties",
                    e
            );
        }
    }

    public static void main(String[] args) throws Exception {

        String searchApiBaseUrl = loadSearchApiBaseUrl();

        System.out.println(
                "Search API URL: " + searchApiBaseUrl
        );

        LogIngestionServiceImpl ingestionService =
                new LogIngestionServiceImpl(searchApiBaseUrl);

        IngestionMetrics metrics = ingestionService.getMetrics();

        Server grpcServer = ServerBuilder
                .forPort(GRPC_PORT)
                .addService(ingestionService)
                .build();

        HttpServer healthServer = HttpServer.create(
                new InetSocketAddress(HEALTH_PORT),
                0
        );

        healthServer.createContext(
                "/health",
                exchange -> handleHealthRequest(exchange, metrics)
        );

        healthServer.start();
        grpcServer.start();

        System.out.println(
                "Log Ingestion gRPC server started on port "
                        + GRPC_PORT
        );

        System.out.println(
                "Log Ingestion health endpoint: http://localhost:"
                        + HEALTH_PORT
                        + "/health"
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            System.out.println(
                    "Shutting down Log Ingestion servers..."
            );

            healthServer.stop(0);
            grpcServer.shutdown();
        }));

        grpcServer.awaitTermination();
    }

    private static void handleHealthRequest(
            HttpExchange exchange,
            IngestionMetrics metrics) throws IOException {

        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(
                    exchange,
                    405,
                    "{\"status\":\"METHOD_NOT_ALLOWED\"}"
            );
            return;
        }

        String response = String.format(
                "{\"status\":\"UP\",\"receivedLogs\":%d,\"forwardedLogs\":%d,\"failedLogs\":%d,\"forwardedBatches\":%d}",
                metrics.getReceivedLogs(),
                metrics.getForwardedLogs(),
                metrics.getFailedLogs(),
                metrics.getForwardedBatches()
        );

        sendResponse(exchange, 200, response);
    }

    private static void sendResponse(
            HttpExchange exchange,
            int statusCode,
            String response) throws IOException {

        byte[] responseBytes =
                response.getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().set(
                "Content-Type",
                "application/json"
        );

        exchange.sendResponseHeaders(
                statusCode,
                responseBytes.length
        );

        try (OutputStream outputStream =
                     exchange.getResponseBody()) {

            outputStream.write(responseBytes);
        }
    }
}