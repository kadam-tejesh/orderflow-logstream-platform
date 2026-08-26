package com.orderflow.loginestion;

import com.orderflow.loginestion.service.LogIngestionServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.nio.file.Path;

public class LogIngestionServer {

    private static final int PORT = 9090;

    public static void main(String[] args) throws Exception {

        Server server = ServerBuilder
                .forPort(PORT)
                .addService(
                        new LogIngestionServiceImpl(
                                Path.of("data", "lucene-index")
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