package com.orderflow.loginestion.service;

import com.orderflow.loginestion.client.LogForwardingClient;
import com.orderflow.loginestion.grpc.LogIngestionServiceGrpc;
import com.orderflow.loginestion.grpc.LogRequest;
import com.orderflow.loginestion.grpc.LogResponse;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LogIngestionServiceImpl
        extends LogIngestionServiceGrpc.LogIngestionServiceImplBase {

    private static final int BATCH_SIZE = 100;
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

        /*
         * A real gRPC server provides ServerCallStreamObserver.
         *
         * Unit tests may provide a plain StreamObserver, so never
         * blindly cast the observer.
         */
        ServerCallStreamObserver<LogResponse> serverObserver = null;

        if (responseObserver instanceof ServerCallStreamObserver<?>) {
            @SuppressWarnings("unchecked")
            ServerCallStreamObserver<LogResponse> typedObserver =
                    (ServerCallStreamObserver<LogResponse>) responseObserver;

            serverObserver = typedObserver;

            /*
             * Enable explicit inbound flow control.
             *
             * This means the server controls how many client messages
             * gRPC is allowed to deliver instead of receiving an
             * unlimited stream into the application.
             */
            serverObserver.disableAutoInboundFlowControl();

            /*
             * Start with one batch worth of messages.
             *
             * Additional messages are requested only after downstream
             * processing creates capacity again.
             */
            serverObserver.request(BATCH_SIZE);
        }

        final ServerCallStreamObserver<LogResponse> finalServerObserver =
                serverObserver;

        ArrayBlockingQueue<LogForwardingClient.ParsedLogData> queue =
                new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);

        Object stateLock = new Object();

        return new StreamObserver<>() {

            private final AtomicInteger receivedLogs =
                    new AtomicInteger();

            private boolean streamFailed = false;
            private boolean inboundCompleted = false;
            private boolean responseSent = false;
            private boolean workerStarted = false;

            /*
             * Used only by the fallback path for unit tests.
             *
             * The real gRPC path uses the bounded queue and worker.
             */
            private final List<LogForwardingClient.ParsedLogData>
                    fallbackBatch = new ArrayList<>(BATCH_SIZE);

            @Override
            public void onNext(LogRequest request) {

                synchronized (stateLock) {

                    if (streamFailed || inboundCompleted || responseSent) {
                        return;
                    }
                }

                metrics.recordReceived();

                try {

                    LogForwardingClient.ParsedLogData parsedLog =
                            parseLog(request);

                    /*
                     * Unit-test fallback.
                     *
                     * Existing tests directly invoke the returned
                     * StreamObserver with a plain TestResponseObserver.
                     * Preserve the original synchronous batching behavior
                     * in that environment.
                     */
                    if (finalServerObserver == null) {

                        fallbackBatch.add(parsedLog);
                        receivedLogs.incrementAndGet();

                        if (fallbackBatch.size() >= BATCH_SIZE) {
                            forwardFallbackBatch();
                        }

                        return;
                    }

                    /*
                     * Production gRPC path.
                     *
                     * Put the parsed log into the bounded queue.
                     * The worker owns all downstream HTTP calls.
                     */
                    boolean accepted = queue.offer(
                            parsedLog,
                            QUEUE_OFFER_TIMEOUT_SECONDS,
                            TimeUnit.SECONDS
                    );

                    if (!accepted) {

                        failStream(
                                responseObserver,
                                Status.RESOURCE_EXHAUSTED,
                                "Ingestion queue is full. "
                                        + "Downstream Search API is too slow."
                        );

                        return;
                    }

                    receivedLogs.incrementAndGet();

                    synchronized (stateLock) {

                        if (!workerStarted) {

                            workerStarted = true;

                            Thread worker = new Thread(
                                    () -> processQueue(
                                            queue,
                                            responseObserver,
                                            finalServerObserver,
                                            stateLock
                                    ),
                                    "log-ingestion-batch-worker"
                            );

                            worker.setDaemon(true);
                            worker.start();
                        }
                    }

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();

                    failStream(
                            responseObserver,
                            Status.CANCELLED,
                            "Log ingestion interrupted"
                    );

                } catch (Exception e) {

                    failStream(
                            responseObserver,
                            Status.INTERNAL,
                            "Failed to process streamed log: "
                                    + e.getMessage()
                    );
                }
            }

            @Override
            public void onError(Throwable throwable) {

                synchronized (stateLock) {

                    if (streamFailed || responseSent) {
                        return;
                    }

                    streamFailed = true;
                }

                metrics.recordFailed();

                System.err.println(
                        "Log stream error: " + throwable.getMessage()
                );

                responseObserver.onError(
                        Status.CANCELLED
                                .withDescription(
                                        "Log stream failed: "
                                                + throwable.getMessage()
                                )
                                .withCause(throwable)
                                .asRuntimeException()
                );
            }

            @Override
            public void onCompleted() {

                /*
                 * Unit-test fallback.
                 *
                 * Flush the final partial batch synchronously before
                 * returning the successful response.
                 */
                if (finalServerObserver == null) {

                    synchronized (stateLock) {

                        if (streamFailed
                                || inboundCompleted
                                || responseSent) {
                            return;
                        }

                        inboundCompleted = true;
                    }

                    try {

                        if (!fallbackBatch.isEmpty()) {
                            forwardFallbackBatch();
                        }

                        sendSuccessResponse(
                                responseObserver,
                                receivedLogs.get()
                        );

                    } catch (Exception e) {

                        failStream(
                                responseObserver,
                                Status.INTERNAL,
                                "Failed to forward final log batch: "
                                        + e.getMessage()
                        );
                    }

                    return;
                }

                /*
                 * Real gRPC path.
                 *
                 * Mark the inbound stream complete. The worker continues
                 * draining the queue and sends the final response after
                 * all queued logs have been forwarded.
                 */
                synchronized (stateLock) {

                    if (streamFailed
                            || inboundCompleted
                            || responseSent) {
                        return;
                    }

                    inboundCompleted = true;
                }

                /*
                 * If no worker was started, there is no queued work to
                 * drain, so the server can respond immediately.
                 */
                boolean shouldRespondImmediately;

                synchronized (stateLock) {
                    shouldRespondImmediately =
                            !workerStarted && !responseSent;
                }

                if (shouldRespondImmediately) {
                    sendSuccessResponse(
                            responseObserver,
                            receivedLogs.get()
                    );
                }
            }

            private void forwardFallbackBatch() throws Exception {

                if (fallbackBatch.isEmpty()) {
                    return;
                }

                List<LogForwardingClient.ParsedLogData> batch =
                        new ArrayList<>(fallbackBatch);

                fallbackBatch.clear();

                forwardingClient.forwardLogs(batch);

                metrics.recordForwarded(batch.size());
                metrics.recordBatchForwarded();

                System.out.println(
                        "Forwarded log batch of "
                                + batch.size()
                                + " logs to Search API"
                );
            }

            private void processQueue(
                    ArrayBlockingQueue<
                            LogForwardingClient.ParsedLogData> queue,
                    StreamObserver<LogResponse> responseObserver,
                    ServerCallStreamObserver<LogResponse> serverObserver,
                    Object stateLock) {

                try {

                    while (true) {

                        synchronized (stateLock) {

                            if (streamFailed || responseSent) {
                                return;
                            }
                        }

                        /*
                         * Wait briefly for the next log.
                         *
                         * This also allows the worker to notice stream
                         * completion when the queue becomes empty.
                         */
                        LogForwardingClient.ParsedLogData first =
                                queue.poll(
                                        100,
                                        TimeUnit.MILLISECONDS
                                );

                        if (first == null) {

                            boolean shouldComplete;

                            synchronized (stateLock) {
                                shouldComplete =
                                        inboundCompleted
                                                && queue.isEmpty()
                                                && !responseSent
                                                && !streamFailed;
                            }

                            if (shouldComplete) {

                                sendSuccessResponse(
                                        responseObserver,
                                        receivedLogs.get()
                                );

                                return;
                            }

                            continue;
                        }

                        List<LogForwardingClient.ParsedLogData> batch =
                                new ArrayList<>(BATCH_SIZE);

                        batch.add(first);

                        queue.drainTo(
                                batch,
                                BATCH_SIZE - 1
                        );

                        /*
                         * Downstream HTTP work happens entirely on this
                         * worker thread rather than the gRPC callback.
                         */
                        forwardingClient.forwardLogs(batch);

                        metrics.recordForwarded(batch.size());
                        metrics.recordBatchForwarded();

                        System.out.println(
                                "Forwarded log batch of "
                                        + batch.size()
                                        + " logs to Search API"
                        );

                        /*
                         * One window of BATCH_SIZE messages has now been
                         * consumed. Allow gRPC to deliver another window.
                         *
                         * Because auto inbound flow control was disabled,
                         * this is what controls the amount of work entering
                         * the application.
                         */
                        serverObserver.request(BATCH_SIZE);
                    }

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();

                    failStream(
                            responseObserver,
                            Status.CANCELLED,
                            "Log ingestion worker interrupted"
                    );

                } catch (Exception e) {

                    failStream(
                            responseObserver,
                            Status.INTERNAL,
                            "Failed to forward log batch: "
                                    + e.getMessage()
                    );
                }
            }

            private void failStream(
                    StreamObserver<LogResponse> responseObserver,
                    Status status,
                    String description) {

                synchronized (stateLock) {

                    if (streamFailed || responseSent) {
                        return;
                    }

                    streamFailed = true;
                }

                metrics.recordFailed();

                responseObserver.onError(
                        status
                                .withDescription(description)
                                .asRuntimeException()
                );
            }

            private void sendSuccessResponse(
                    StreamObserver<LogResponse> responseObserver,
                    int totalLogs) {

                synchronized (stateLock) {

                    if (streamFailed || responseSent) {
                        return;
                    }

                    responseSent = true;
                }

                LogResponse response = LogResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage(
                                totalLogs
                                        + " logs received and forwarded for indexing"
                        )
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
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