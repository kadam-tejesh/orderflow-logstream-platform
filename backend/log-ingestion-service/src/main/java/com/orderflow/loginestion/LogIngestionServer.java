package com.orderflow.loginestion;

import com.orderflow.loginestion.service.LogIngestionServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.InputStream;
import java.util.Properties;

public class LogIngestionServer {

    private static final int PORT = 9090;

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

        Server server = ServerBuilder
                .forPort(PORT)
                .addService(
                        new LogIngestionServiceImpl(
                                searchApiBaseUrl
                        )
                )
                .build();

        server.start();

        System.out.println(
                "Log Ingestion gRPC server started on port " + PORT
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            System.out.println(
                    "Shutting down Log Ingestion server..."
            );

            server.shutdown();
        }));

        server.awaitTermination();
    }
}