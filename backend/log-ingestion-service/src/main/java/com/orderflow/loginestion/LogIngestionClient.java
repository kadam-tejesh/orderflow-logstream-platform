package com.orderflow.loginestion;

import com.orderflow.loginestion.grpc.LogIngestionServiceGrpc;
import com.orderflow.loginestion.grpc.LogRequest;
import com.orderflow.loginestion.grpc.LogResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class LogIngestionClient {

    public static void main(String[] args) {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 9090)
                .usePlaintext()
                .build();

        try {
            LogIngestionServiceGrpc.LogIngestionServiceBlockingStub stub =
                    LogIngestionServiceGrpc.newBlockingStub(channel);

            LogRequest request = LogRequest.newBuilder()
                    .setTimestamp("2026-08-25T10:15:00")
                    .setLevel("INFO")
                    .setService("order-service")
                    .setMessage("Test order log for Lucene indexing")
                    .build();

            LogResponse response = stub.sendLog(request);

            System.out.println("Server response:");
            System.out.println(response.getMessage());

        } finally {
            channel.shutdown();
        }
    }
}