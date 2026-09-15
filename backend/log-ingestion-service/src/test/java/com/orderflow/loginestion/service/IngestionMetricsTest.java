package com.orderflow.loginestion.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IngestionMetricsTest {

    @Test
    void shouldRecordReceivedLogs() {
        IngestionMetrics metrics = new IngestionMetrics();

        metrics.recordReceived();
        metrics.recordReceived();
        metrics.recordReceived();

        assertEquals(3, metrics.getReceivedLogs());
    }

    @Test
    void shouldRecordForwardedLogs() {
        IngestionMetrics metrics = new IngestionMetrics();

        metrics.recordForwarded(100);
        metrics.recordForwarded(50);

        assertEquals(150, metrics.getForwardedLogs());
    }

    @Test
    void shouldRecordFailedLogs() {
        IngestionMetrics metrics = new IngestionMetrics();

        metrics.recordFailed();
        metrics.recordFailed();

        assertEquals(2, metrics.getFailedLogs());
    }

    @Test
    void shouldRecordForwardedBatches() {
        IngestionMetrics metrics = new IngestionMetrics();

        metrics.recordBatchForwarded();
        metrics.recordBatchForwarded();
        metrics.recordBatchForwarded();

        assertEquals(3, metrics.getForwardedBatches());
    }

    @Test
    void shouldStartWithZeroMetrics() {
        IngestionMetrics metrics = new IngestionMetrics();

        assertEquals(0, metrics.getReceivedLogs());
        assertEquals(0, metrics.getForwardedLogs());
        assertEquals(0, metrics.getFailedLogs());
        assertEquals(0, metrics.getForwardedBatches());
    }
}
