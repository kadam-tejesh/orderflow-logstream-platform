package com.orderflow.loginestion.service;

import com.orderflow.loginestion.grpc.LogIngestionServiceGrpc;
import com.orderflow.loginestion.grpc.LogRequest;
import com.orderflow.loginestion.grpc.LogResponse;
import io.grpc.stub.StreamObserver;

public class LogIngestionServiceImpl
        extends LogIngestionServiceGrpc.LogIngestionServiceImplBase {

    @Override
    public void sendLog(
            LogRequest request,
            StreamObserver<LogResponse> responseObserver) {

        processLog(request);

        LogResponse response = LogResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Log received successfully")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<LogRequest> streamLogs(
            StreamObserver<LogResponse> responseObserver) {

        return new StreamObserver<>() {

            private int receivedLogs = 0;

            @Override
            public void onNext(LogRequest request) {
                processLog(request);
                receivedLogs++;
            }

            @Override
            public void onError(Throwable throwable) {
                System.err.println(
                        "Log stream error: " + throwable.getMessage()
                );
            }

            @Override
            public void onCompleted() {

                LogResponse response = LogResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage(
                                receivedLogs +
                                " logs received successfully"
                        )
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    private void processLog(LogRequest request) {

        System.out.printf(
                "[%s] [%s] [%s] %s%n",
                request.getTimestamp(),
                request.getLevel(),
                request.getService(),
                request.getMessage()
        );
    }
}
