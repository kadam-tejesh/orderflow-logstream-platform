package com.orderflow.loginestion.service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Runtime metrics for the log ingestion pipeline.
 */
public class IngestionMetrics {

    private final AtomicLong receivedLogs = new AtomicLong();
    private final AtomicLong forwardedLogs = new AtomicLong();
    private final AtomicLong failedLogs = new AtomicLong();
    private final AtomicLong forwardedBatches = new AtomicLong();

    public void recordReceived() {
        receivedLogs.incrementAndGet();
    }

    public void recordForwarded(long count) {
        forwardedLogs.addAndGet(count);
    }

    public void recordFailed() {
        failedLogs.incrementAndGet();
    }

    public void recordBatchForwarded() {
        forwardedBatches.incrementAndGet();
    }

    public long getReceivedLogs() {
        return receivedLogs.get();
    }

    public long getForwardedLogs() {
        return forwardedLogs.get();
    }

    public long getFailedLogs() {
        return failedLogs.get();
    }

    public long getForwardedBatches() {
        return forwardedBatches.get();
    }
}
