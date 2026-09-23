package com.orderflow.loginestion.service;

import com.orderflow.loginestion.client.LogForwardingClient;
import com.orderflow.loginestion.grpc.LogIngestionServiceGrpc;
import com.orderflow.loginestion.grpc.LogRequest;
import com.orderflow.loginestion.grpc.LogResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LogIngestionServiceImpl
        extends LogIngestionServiceGrpc.LogIngestionServiceImplBase {

    private static final int BATCH_SIZE = 100;

    /*
     * Maximum number of parsed logs that can wait for forwarding.
     *
     * This provides a bounded buffer so memory usage does not grow
     * without limit when log producers are faster than the
     * Search & Indexing Service.
     */
    private static final int MAX_QUEUE_SIZE = 1000;

    private static final long QUEUE_OFFER_TIMEOUT_SECONDS = 5;

    private final LogForwardingClient forwardingClient;
    private final LogParser logParser;
    private final IngestionMetrics metrics;

    public LogIngestionServiceImpl(String searchApiBaseUrl) {
        this.forwardingClient = new LogForwardingClient(searchApiBaseUrl);
        this.logParser = new LogParser();
        this.metrics = new IngestionMetrics();
    }

    public IngestionMetrics getMetrics() {
        return metrics;
    }

    @Override
    public void sendLog(
            LogRequest request,
            StreamObserver<LogResponse> responseObserver) {

        metrics.recordReceived();

        try {
            processLog(request);
            metrics.recordForwarded(1);

            LogResponse response = LogResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Log received and forwarded for indexing")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {

            metrics.recordFailed();

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

            /*
             * Bounded queue prevents unlimited memory growth when
             * incoming logs are faster than downstream forwarding.
             */
            private final ArrayBlockingQueue<
                    LogForwardingClient.ParsedLogData> queue =
                    new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);

            private int receivedLogs = 0;
            private boolean streamFailed = false;

            @Override
            public void onNext(LogRequest request) {

                if (streamFailed) {
                    return;
                }

                metrics.recordReceived();

                try {
                    LogForwardingClient.ParsedLogData parsedLog =
                            parseLog(request);

                    /*
                     * Add the parsed log to the bounded queue.
                     *
                     * If the queue is full, wait for downstream
                     * capacity instead of allowing unlimited memory
                     * growth.
                     */
                    boolean accepted = queue.offer(
                            parsedLog,
                            QUEUE_OFFER_TIMEOUT_SECONDS,
                            TimeUnit.SECONDS
                    );

                    if (!accepted) {
                        throw new IllegalStateException(
                                "Ingestion queue is full. "
                                        + "Downstream Search API is too slow."
                        );
                    }

                    receivedLogs++;

                    /*
                     * Flush complete batches.
                     */
                    if (queue.size() >= BATCH_SIZE) {
                        flushBatch();
                    }

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();

                    streamFailed = true;
                    metrics.recordFailed();

                    responseObserver.onError(
                            Status.CANCELLED
                                    .withDescription(
                                            "Log ingestion interrupted"
                                    )
                                    .withCause(e)
                                    .asRuntimeException()
                    );

                } catch (Exception e) {

                    streamFailed = true;
                    metrics.recordFailed();

                    System.err.println(
                            "Failed to process streamed log:"
                    );
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
                    metrics.recordFailed();
                    responseObserver.onError(throwable);
                }
            }

            @Override
            public void onCompleted() {

                if (streamFailed) {
                    return;
                }

                try {
                    /*
                     * Flush any remaining logs smaller than BATCH_SIZE.
                     */
                    while (!queue.isEmpty()) {
                        flushBatch();
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

                } catch (Exception e) {

                    streamFailed = true;
                    metrics.recordFailed();

                    System.err.println(
                            "Failed to flush final log batch:"
                    );
                    e.printStackTrace();

                    responseObserver.onError(
                            Status.INTERNAL
                                    .withDescription(
                                            "Failed to flush final log batch: "
                                                    + e.getMessage()
                                    )
                                    .withCause(e)
                                    .asRuntimeException()
                    );
                }
            }

            private void flushBatch() throws Exception {

                if (queue.isEmpty()) {
                    return;
                }

                List<LogForwardingClient.ParsedLogData> batch =
                        new ArrayList<>(BATCH_SIZE);

                queue.drainTo(batch, BATCH_SIZE);

                if (batch.isEmpty()) {
                    return;
                }

                int batchSize = batch.size();

                forwardingClient.forwardLogs(batch);

                metrics.recordForwarded(batchSize);
                metrics.recordBatchForwarded();

                System.out.println(
                        "Forwarded log batch of "
                                + batchSize
                                + " logs to Search API"
                );
            }
        };
    }

    private void processLog(LogRequest request) throws Exception {

        LogForwardingClient.ParsedLogData parsedLog =
                parseLog(request);

        forwardingClient.forwardLog(
                parsedLog.timestamp(),
                parsedLog.level(),
                parsedLog.service(),
                parsedLog.message()
        );
    }

    private LogForwardingClient.ParsedLogData parseLog(
            LogRequest request) {

        LogParser.ParsedLog parsedLog = logParser.parse(
                request.getTimestamp(),
                request.getLevel(),
                request.getService(),
                request.getMessage()
        );

        return new LogForwardingClient.ParsedLogData(
                parsedLog.getTimestamp(),
                parsedLog.getLevel(),
                parsedLog.getService(),
                parsedLog.getMessage()
        );
    }
}