package com.orderflow.loginestion.service;

import com.orderflow.loginestion.grpc.LogIngestionServiceGrpc;
import com.orderflow.loginestion.grpc.LogRequest;
import com.orderflow.loginestion.grpc.LogResponse;
import com.orderflow.loginestion.index.LuceneLogIndexer;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.file.Path;

public class LogIngestionServiceImpl
        extends LogIngestionServiceGrpc.LogIngestionServiceImplBase {

    private final LuceneLogIndexer indexer;

    public LogIngestionServiceImpl(Path indexPath) throws IOException {
        this.indexer = new LuceneLogIndexer(indexPath);
    }

    @Override
    public void sendLog(
            LogRequest request,
            StreamObserver<LogResponse> responseObserver) {

        try {
            processLog(request);

            LogResponse response = LogResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Log received and indexed successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public StreamObserver<LogRequest> streamLogs(
            StreamObserver<LogResponse> responseObserver) {

        return new StreamObserver<>() {

            private int receivedLogs = 0;

            @Override
            public void onNext(LogRequest request) {
                try {
                    processLog(request);
                    receivedLogs++;
                } catch (IOException e) {
                    responseObserver.onError(e);
                }
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
                                " logs received and indexed successfully"
                        )
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    private void processLog(LogRequest request) throws IOException {

        System.out.printf(
                "[%s] [%s] [%s] %s%n",
                request.getTimestamp(),
                request.getLevel(),
                request.getService(),
                request.getMessage()
        );

        indexer.indexLog(
                request.getTimestamp(),
                request.getLevel(),
                request.getService(),
                request.getMessage()
        );
    }
}