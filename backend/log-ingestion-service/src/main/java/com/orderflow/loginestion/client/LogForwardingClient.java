package com.orderflow.loginestion.client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Forwards parsed logs to the Search & Indexing Engine.
 */
public class LogForwardingClient {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final int EXPECTED_SUCCESS_STATUS = 201;

    private final HttpClient httpClient;
    private final String searchApiBaseUrl;

    public LogForwardingClient(String searchApiBaseUrl) {

        if (searchApiBaseUrl == null || searchApiBaseUrl.isBlank()) {
            throw new IllegalArgumentException(
                    "Search API base URL must not be empty"
            );
        }

        this.searchApiBaseUrl = searchApiBaseUrl.replaceAll("/+$", "");

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
    }

    /**
     * Forwards a single log to the Search API.
     */
    public void forwardLog(
            String timestamp,
            String level,
            String service,
            String message) throws Exception {

        ParsedLogData log = new ParsedLogData(
                timestamp,
                level,
                service,
                message
        );

        sendRequest(
                "/api/index",
                buildSingleLogJson(log)
        );
    }

    /**
     * Forwards multiple logs in one HTTP request.
     *
     * This reduces HTTP request overhead and allows the search service
     * to index and commit the entire batch together.
     */
    public void forwardLogs(List<ParsedLogData> logs) throws Exception {

        if (logs == null || logs.isEmpty()) {
            return;
        }

        StringBuilder json = new StringBuilder();
        json.append("{\"logs\":[");

        for (int i = 0; i < logs.size(); i++) {

            if (i > 0) {
                json.append(",");
            }

            json.append(buildSingleLogJson(logs.get(i)));
        }

        json.append("]}");

        sendRequest(
                "/api/index/batch",
                json.toString()
        );
    }

    private void sendRequest(
            String endpoint,
            String json) throws Exception {

        HttpRequest request;

        try {
            request = HttpRequest.newBuilder()
                    .uri(new URI(searchApiBaseUrl + endpoint))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .timeout(REQUEST_TIMEOUT)
                    .build();

        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Invalid Search API URL: " + searchApiBaseUrl,
                    e
            );
        }

        try {
            HttpResponse<String> response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.ofString()
            );

            if (response.statusCode() != EXPECTED_SUCCESS_STATUS) {
                throw new RuntimeException(
                        "Search API returned status "
                                + response.statusCode()
                                + ": "
                                + response.body()
                );
            }

            System.out.println(
                    "Successfully forwarded "
                            + (endpoint.endsWith("/batch")
                            ? "log batch"
                            : "log")
                            + " to Search API"
            );

        } catch (IOException e) {
            throw new RuntimeException(
                    "Could not connect to Search API at "
                            + searchApiBaseUrl,
                    e
            );

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(
                    "Log forwarding request was interrupted",
                    e
            );
        }
    }

    private String buildSingleLogJson(ParsedLogData log) {

        long timestampMillis = parseTimestamp(log.timestamp());

        return String.format(
                "{\"level\":\"%s\",\"service\":\"%s\",\"timestamp\":%d,\"message\":\"%s\",\"responseTime\":0}",
                escape(log.level()),
                escape(log.service()),
                timestampMillis,
                escape(log.message())
        );
    }

    /**
     * Converts the incoming timestamp into Unix epoch milliseconds.
     *
     * Supported formats:
     * 1. Numeric Unix timestamp in milliseconds
     * 2. ISO-8601 LocalDateTime
     *
     * If the timestamp cannot be parsed, the current system time is used.
     */
    private long parseTimestamp(String timestamp) {

        if (timestamp == null || timestamp.isBlank()) {
            return System.currentTimeMillis();
        }

        try {
            return Long.parseLong(timestamp);

        } catch (NumberFormatException ignored) {
            // Try ISO-8601 timestamp below.
        }

        try {
            return java.time.LocalDateTime
                    .parse(timestamp)
                    .atZone(java.time.ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();

        } catch (java.time.format.DateTimeParseException e) {

            System.err.println(
                    "Could not parse timestamp '"
                            + timestamp
                            + "', using current time instead"
            );

            return System.currentTimeMillis();
        }
    }

    /**
     * Escapes values before inserting them into the JSON request body.
     */
    private String escape(String value) {

        return Objects.toString(value, "")
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }

    /**
     * Represents one parsed log ready for forwarding.
     */
    public record ParsedLogData(
            String timestamp,
            String level,
            String service,
            String message) {
    }
}