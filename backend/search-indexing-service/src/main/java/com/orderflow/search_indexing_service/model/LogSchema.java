package com.orderflow.search_indexing_service.model;

/**
 * Field-name constants for the log document schema.
 * level          - exact-match keyword (e.g. ERROR, INFO, WARN)
 * service        - exact-match keyword (e.g. billing-api)
 * timestamp      - epoch millis, indexed as a point for range queries
 * message        - full-text, analyzed and searchable
 * response_time  - int millis, indexed as a point for range queries (e.g. > 1000)
 */
public final class LogSchema {
    public static final String LEVEL = "level";
    public static final String SERVICE = "service";
    public static final String TIMESTAMP = "timestamp";
    public static final String MESSAGE = "message";
    public static final String RESPONSE_TIME = "response_time";

    private LogSchema() {
    }
}