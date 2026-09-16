package com.orderflow.loginestion.benchmark;

import com.orderflow.loginestion.grpc.LogIngestionServiceGrpc;
import com.orderflow.loginestion.grpc.LogRequest;
import com.orderflow.loginestion.grpc.LogResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LogIngestionPerformanceBenchmark {

    private static final String HOST = "localhost";
    private static final int PORT = 9090;
    private static final int LOG_COUNT = 10_000;

    public static void main(String[] args) throws Exception {

        System.out.println("==========================================");
        System.out.println("Log Ingestion Performance Benchmark");
        System.out.println("==========================================");
        System.out.println("Target logs: " + LOG_COUNT);
        System.out.println("Server: " + HOST + ":" + PORT);
        System.out.println();

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(HOST, PORT)
                .usePlaintext()
                .build();

        CountDownLatch completed = new CountDownLatch(1);

        try {

            LogIngestionServiceGrpc.LogIngestionServiceStub stub =
                    LogIngestionServiceGrpc.newStub(channel);

            final long[] startTime = new long[1];
            final long[] endTime = new long[1];

            final boolean[] success = new boolean[1];
            final String[] serverMessage = new String[1];
            final Throwable[] error = new Throwable[1];

            StreamObserver<LogResponse> responseObserver =
                    new StreamObserver<>() {

                        @Override
                        public void onNext(LogResponse response) {
                            success[0] = response.getSuccess();
                            serverMessage[0] = response.getMessage();
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            error[0] = throwable;
                            endTime[0] = System.nanoTime();
                            completed.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            endTime[0] = System.nanoTime();
                            completed.countDown();
                        }
                    };

            StreamObserver<LogRequest> requestObserver =
                    stub.streamLogs(responseObserver);

            startTime[0] = System.nanoTime();

            for (int i = 1; i <= LOG_COUNT; i++) {

                LogRequest request = LogRequest.newBuilder()
                        .setTimestamp("2026-09-15T19:00:00")
                        .setLevel("INFO")
                        .setService("benchmark-service")
                        .setMessage("Performance benchmark log " + i)
                        .build();

                requestObserver.onNext(request);
            }

            requestObserver.onCompleted();

            boolean finished = completed.await(120, TimeUnit.SECONDS);

            if (!finished) {
                System.err.println("Benchmark timed out after 120 seconds.");
                return;
            }

            if (error[0] != null) {
                System.err.println("Benchmark failed:");
                error[0].printStackTrace();
                return;
            }

            double elapsedSeconds =
                    (endTime[0] - startTime[0]) / 1_000_000_000.0;

            double throughput =
                    LOG_COUNT / elapsedSeconds;

            System.out.println();
            System.out.println("==========================================");
            System.out.println("Benchmark Result");
            System.out.println("==========================================");
            System.out.println("Total logs       : " + LOG_COUNT);
            System.out.printf(
                    "Elapsed time     : %.3f seconds%n",
                    elapsedSeconds
            );
            System.out.printf(
                    "Throughput       : %.2f logs/sec%n",
                    throughput
            );
            System.out.println("Server success   : " + success[0]);
            System.out.println("Server message   : " + serverMessage[0]);
            System.out.println();

            if (throughput >= 10_000) {
                System.out.println("RESULT: 10,000 logs/sec target achieved.");
            } else {
                System.out.printf(
                        "RESULT: Target not reached. Current throughput: %.2f logs/sec%n",
                        throughput
                );
            }

            System.out.println("==========================================");

        } finally {
            channel.shutdown();

            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        }
    }
}