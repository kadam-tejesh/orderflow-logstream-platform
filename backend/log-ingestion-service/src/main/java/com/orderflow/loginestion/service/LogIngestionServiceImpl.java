package com.orderflow.loginestion.service;

import com.orderflow.loginestion.client.LogForwardingClient;
import com.orderflow.loginestion.grpc.LogIngestionServiceGrpc;
import com.orderflow.loginestion.grpc.LogRequest;
import com.orderflow.loginestion.grpc.LogResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class LogIngestionServiceImpl
        extends LogIngestionServiceGrpc.LogIngestionServiceImplBase {

    private final LogForwardingClient forwardingClient;
    private final LogParser logParser;

    public LogIngestionServiceImpl(String searchApiBaseUrl) {
        this.forwardingClient = new LogForwardingClient(searchApiBaseUrl);
        this.logParser = new LogParser();
    }

    @Override
    public void sendLog(
            LogRequest request,
            StreamObserver<LogResponse> responseObserver) {

        try {
            processLog(request);

            LogResponse response = LogResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Log received and forwarded for indexing")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {

            System.err.println("Failed to process log:");
            e.printStackTrace();

            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(
                                    "Failed to process log: " + e.getMessage()
                            )
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public StreamObserver<LogRequest> streamLogs(
            StreamObserver<LogResponse> responseObserver) {

        return new StreamObserver<>() {

            private int receivedLogs = 0;
            private boolean streamFailed = false;

            @Override
            public void onNext(LogRequest request) {

                if (streamFailed) {
                    return;
                }

                try {
                    processLog(request);
                    receivedLogs++;

                } catch (Exception e) {

                    streamFailed = true;

                    System.err.println("Failed to process streamed log:");
                    e.printStackTrace();

                    responseObserver.onError(
                            Status.INTERNAL
                                    .withDescription(
                                            "Failed to process streamed log: "
                                                    + e.getMessage()
                                    )
                                    .withCause(e)
                                    .asRuntimeException()
                    );
                }
            }

            @Override
            public void onError(Throwable throwable) {

                System.err.println(
                        "Log stream error: " + throwable.getMessage()
                );

                if (!streamFailed) {
                    responseObserver.onError(throwable);
                }
            }

            @Override
            public void onCompleted() {

                if (streamFailed) {
                    return;
                }

                LogResponse response = LogResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage(
                                receivedLogs
                                        + " logs received and forwarded for indexing"
                        )
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    private void processLog(LogRequest request) throws Exception {

        LogParser.ParsedLog parsedLog = logParser.parse(
                request.getTimestamp(),
                request.getLevel(),
                request.getService(),
                request.getMessage()
        );

        System.out.printf(
                "[%s] [%s] [%s] %s%n",
                parsedLog.getTimestamp(),
                parsedLog.getLevel(),
                parsedLog.getService(),
                parsedLog.getMessage()
        );

        forwardingClient.forwardLog(
                parsedLog.getTimestamp(),
                parsedLog.getLevel(),
                parsedLog.getService(),
                parsedLog.getMessage()
        );
    }
}