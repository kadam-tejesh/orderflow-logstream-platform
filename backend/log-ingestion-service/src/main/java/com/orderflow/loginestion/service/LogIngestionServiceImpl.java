package com.orderflow.loginestion.service;

import com.orderflow.loginestion.client.LogForwardingClient;
import com.orderflow.loginestion.client.LogForwardingClient.ParsedLogData;
import com.orderflow.loginestion.grpc.LogIngestionServiceGrpc;
import com.orderflow.loginestion.grpc.LogRequest;
import com.orderflow.loginestion.grpc.LogResponse;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogIngestionServiceImpl
        extends LogIngestionServiceGrpc.LogIngestionServiceImplBase {

    private static final int BATCH_SIZE = 100;

    private static final int MAX_QUEUE_SIZE = 1000;

    private static final long QUEUE_OFFER_TIMEOUT_SECONDS = 5;

    private final LogForwardingClient forwardingClient;

    private final IngestionMetrics metrics;

    public LogIngestionServiceImpl(String searchApiBaseUrl) {

        this.forwardingClient =
                new LogForwardingClient(searchApiBaseUrl);

        this.metrics =
                new IngestionMetrics();
    }

    public LogIngestionServiceImpl(
            LogForwardingClient forwardingClient,
            IngestionMetrics metrics) {

        this.forwardingClient = forwardingClient;
        this.metrics = metrics;
    }

    public IngestionMetrics getMetrics() {
        return metrics;
    }

    /**
     * Handles a single log request synchronously.
     *
     * This method is used by the single-log API and keeps the response
     * deterministic for callers that expect the forwarding operation
     * to complete before the method returns.
     */
    public void sendLog(
            LogRequest request,
            StreamObserver<LogResponse> responseObserver) {

        metrics.recordReceived();

        ParsedLogData parsedLog =
                normalizeLog(request);

        try {

            forwardingClient.forwardLog(
                    parsedLog.timestamp(),
                    parsedLog.level(),
                    parsedLog.service(),
                    parsedLog.message()
            );

            metrics.recordForwarded(1);

            metrics.recordBatchForwarded();

            responseObserver.onNext(
                    LogResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage(
                                    "Log received and forwarded "
                                            + "for indexing"
                            )
                            .build()
            );

            responseObserver.onCompleted();

        } catch (Exception e) {

            metrics.recordFailed();

            responseObserver.onError(e);
        }
    }

    @Override
    public StreamObserver<LogRequest> streamLogs(
            StreamObserver<LogResponse> responseObserver) {

        /*
         * Real gRPC calls provide ServerCallStreamObserver, allowing us
         * to explicitly control inbound flow and apply backpressure.
         */
        if (responseObserver instanceof ServerCallStreamObserver<?> rawObserver) {

            @SuppressWarnings("unchecked")
            ServerCallStreamObserver<LogResponse> serverObserver =
                    (ServerCallStreamObserver<LogResponse>) rawObserver;

            return createAsyncStreamObserver(
                    serverObserver,
                    responseObserver
            );
        }

        /*
         * Test/local observers use the synchronous implementation.
         */
        return createSynchronousStreamObserver(responseObserver);
    }

    /**
     * Production streaming path.
     *
     * Uses:
     * - bounded queue
     * - asynchronous batch worker
     * - explicit gRPC inbound flow control
     * - downstream backpressure
     */
    private StreamObserver<LogRequest> createAsyncStreamObserver(
            ServerCallStreamObserver<LogResponse> serverObserver,
            StreamObserver<LogResponse> responseObserver) {

        BlockingQueue<LogRequest> queue =
                new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);

        AtomicBoolean streamFailed =
                new AtomicBoolean(false);

        AtomicBoolean inboundCompleted =
                new AtomicBoolean(false);

        AtomicBoolean responseSent =
                new AtomicBoolean(false);

        AtomicBoolean workerStarted =
                new AtomicBoolean(false);

        /*
         * Disable automatic inbound flow control.
         */
        serverObserver.disableAutoInboundFlowControl();

        /*
         * Request the first batch.
         */
        serverObserver.request(BATCH_SIZE);

        Thread worker = new Thread(
                () -> processQueue(
                        queue,
                        serverObserver,
                        responseObserver,
                        streamFailed,
                        inboundCompleted,
                        responseSent
                ),
                "log-ingestion-batch-worker"
        );

        return new StreamObserver<>() {

            @Override
            public void onNext(LogRequest request) {

                if (streamFailed.get()) {
                    return;
                }

                metrics.recordReceived();

                try {

                    boolean added =
                            queue.offer(
                                    request,
                                    QUEUE_OFFER_TIMEOUT_SECONDS,
                                    TimeUnit.SECONDS
                            );

                    if (!added) {

                        failStream(
                                responseObserver,
                                streamFailed,
                                responseSent,
                                new RuntimeException(
                                        "Ingestion queue is full. "
                                                + "Downstream Search API is too slow."
                                )
                        );

                        return;
                    }

                    /*
                     * Start exactly one worker.
                     */
                    if (workerStarted.compareAndSet(false, true)) {
                        worker.start();
                    }

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();

                    failStream(
                            responseObserver,
                            streamFailed,
                            responseSent,
                            new RuntimeException(
                                    "Interrupted while adding log "
                                            + "to ingestion queue",
                                    e
                            )
                    );
                }
            }

            @Override
            public void onError(Throwable throwable) {

                streamFailed.set(true);

                if (workerStarted.get()) {
                    worker.interrupt();
                }

                if (responseSent.compareAndSet(false, true)) {
                    responseObserver.onError(throwable);
                }
            }

            @Override
            public void onCompleted() {

                inboundCompleted.set(true);

                /*
                 * Start the worker even when the stream contains no logs,
                 * so that the response can be completed.
                 */
                if (workerStarted.compareAndSet(false, true)) {
                    worker.start();
                }
            }
        };
    }

    /**
     * Production batch worker.
     */
    private void processQueue(
            BlockingQueue<LogRequest> queue,
            ServerCallStreamObserver<LogResponse> serverObserver,
            StreamObserver<LogResponse> responseObserver,
            AtomicBoolean streamFailed,
            AtomicBoolean inboundCompleted,
            AtomicBoolean responseSent) {

        try {

            while (!streamFailed.get()) {

                /*
                 * Wait briefly for the next log.
                 */
                LogRequest firstRequest =
                        queue.poll(
                                100,
                                TimeUnit.MILLISECONDS
                        );

                if (firstRequest == null) {

                    if (inboundCompleted.get()
                            && queue.isEmpty()) {

                        sendSuccessResponse(
                                responseObserver,
                                responseSent
                        );

                        return;
                    }

                    continue;
                }

                List<LogRequest> batch =
                        new ArrayList<>(BATCH_SIZE);

                batch.add(firstRequest);

                /*
                 * Fill the remainder of the batch without blocking.
                 */
                queue.drainTo(
                        batch,
                        BATCH_SIZE - 1
                );

                List<ParsedLogData> parsedLogs =
                        new ArrayList<>(batch.size());

                for (LogRequest request : batch) {

                    parsedLogs.add(
                            normalizeLog(request)
                    );
                }

                try {

                    forwardingClient.forwardLogs(
                            parsedLogs
                    );

                    metrics.recordForwarded(
                            parsedLogs.size()
                    );

                    metrics.recordBatchForwarded();

                    System.out.println(
                            "Forwarded batch of "
                                    + parsedLogs.size()
                                    + " logs"
                    );

                } catch (Exception e) {

                    metrics.recordFailed();

                    failStream(
                            responseObserver,
                            streamFailed,
                            responseSent,
                            e
                    );

                    return;
                }

                /*
                 * Request another inbound window only after the current
                 * batch has been successfully forwarded.
                 *
                 * This is the main backpressure mechanism.
                 */
                if (!streamFailed.get()) {
                    serverObserver.request(BATCH_SIZE);
                }

                /*
                 * If the client has completed sending and there is
                 * nothing left in the queue, the stream is finished.
                 */
                if (inboundCompleted.get()
                        && queue.isEmpty()) {

                    sendSuccessResponse(
                            responseObserver,
                            responseSent
                    );

                    return;
                }
            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            if (!streamFailed.get()) {

                failStream(
                        responseObserver,
                        streamFailed,
                        responseSent,
                        new RuntimeException(
                                "Ingestion worker interrupted",
                                e
                        )
                );
            }

        } catch (Exception e) {

            failStream(
                    responseObserver,
                    streamFailed,
                    responseSent,
                    e
            );
        }
    }

    /**
     * Synchronous streaming implementation used by tests and
     * non-gRPC observers.
     */
    private StreamObserver<LogRequest> createSynchronousStreamObserver(
            StreamObserver<LogResponse> responseObserver) {

        List<LogRequest> batch =
                new ArrayList<>(BATCH_SIZE);

        AtomicBoolean responseSent =
                new AtomicBoolean(false);

        AtomicBoolean streamFailed =
                new AtomicBoolean(false);

        return new StreamObserver<>() {

            @Override
            public void onNext(LogRequest request) {

                if (streamFailed.get()) {
                    return;
                }

                metrics.recordReceived();

                batch.add(request);

                /*
                 * Forward immediately when the batch reaches 100.
                 */
                if (batch.size() >= BATCH_SIZE) {

                    boolean success =
                            forwardBatch(
                                    batch,
                                    responseObserver,
                                    streamFailed
                            );

                    batch.clear();

                    if (!success) {
                        return;
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {

                streamFailed.set(true);

                if (responseSent.compareAndSet(false, true)) {
                    responseObserver.onError(throwable);
                }
            }

            @Override
            public void onCompleted() {

                if (streamFailed.get()) {
                    return;
                }

                /*
                 * Forward the final partial batch.
                 */
                if (!batch.isEmpty()) {

                    boolean success =
                            forwardBatch(
                                    batch,
                                    responseObserver,
                                    streamFailed
                            );

                    batch.clear();

                    if (!success) {
                        return;
                    }
                }

                sendSuccessResponse(
                        responseObserver,
                        responseSent
                );
            }
        };
    }

    /**
     * Forwards one batch synchronously.
     */
    private boolean forwardBatch(
            List<LogRequest> requests,
            StreamObserver<LogResponse> responseObserver,
            AtomicBoolean streamFailed) {

        List<ParsedLogData> parsedLogs =
                new ArrayList<>(requests.size());

        for (LogRequest request : requests) {

            parsedLogs.add(
                    normalizeLog(request)
            );
        }

        try {

            forwardingClient.forwardLogs(
                    parsedLogs
            );

            metrics.recordForwarded(
                    parsedLogs.size()
            );

            metrics.recordBatchForwarded();

            return true;

        } catch (Exception e) {

            metrics.recordFailed();

            streamFailed.set(true);

            responseObserver.onError(e);

            return false;
        }
    }

    /**
     * Normalizes incoming log fields before forwarding.
     *
     * Examples:
     * "  info " -> "INFO"
     * "  order-service " -> "order-service"
     * "  message  " -> "message"
     */
    private ParsedLogData normalizeLog(
            LogRequest request) {

        String timestamp =
                request.getTimestamp()
                        .trim();

        String level =
                request.getLevel()
                        .trim()
                        .toUpperCase();

        String service =
                request.getService()
                        .trim();

        String message =
                request.getMessage()
                        .trim();

        return new ParsedLogData(
                timestamp,
                level,
                service,
                message
        );
    }

    private void failStream(
            StreamObserver<LogResponse> responseObserver,
            AtomicBoolean streamFailed,
            AtomicBoolean responseSent,
            Throwable throwable) {

        if (!streamFailed.compareAndSet(false, true)) {
            return;
        }

        metrics.recordFailed();

        if (responseSent.compareAndSet(false, true)) {
            responseObserver.onError(throwable);
        }
    }

    private void sendSuccessResponse(
            StreamObserver<LogResponse> responseObserver,
            AtomicBoolean responseSent) {

        if (!responseSent.compareAndSet(false, true)) {
            return;
        }

        long received =
                metrics.getReceivedLogs();

        responseObserver.onNext(
                LogResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage(
                                received
                                        + " logs received and forwarded "
                                        + "for indexing"
                        )
                        .build()
        );

        responseObserver.onCompleted();
    }
}
