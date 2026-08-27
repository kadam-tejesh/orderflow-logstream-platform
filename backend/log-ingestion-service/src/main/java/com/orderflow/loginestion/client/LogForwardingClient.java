package com.orderflow.loginestion.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Forwards parsed logs to the Search & Indexing Engine's /api/index endpoint,
 * instead of indexing locally.
 */
public class LogForwardingClient {

    private final HttpClient httpClient;
    private final String searchApiBaseUrl;

    public LogForwardingClient(String searchApiBaseUrl) {
        this.searchApiBaseUrl = searchApiBaseUrl;

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public void forwardLog(
            String timestamp,
            String level,
            String service,
            String message) throws Exception {

        long timestampMillis = parseTimestamp(timestamp);

        // NOTE: LogRequest proto has no response_time field yet.
        // Defaulting to 0 until the schema is extended.
        String json = String.format(
                "{\"level\":\"%s\",\"service\":\"%s\",\"timestamp\":%d,\"message\":\"%s\",\"responseTime\":0}",
                escape(level),
                escape(service),
                timestampMillis,
                escape(message)
        );

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(searchApiBaseUrl + "/api/index"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .timeout(Duration.ofSeconds(5))
                .build();

        HttpResponse<String> response = httpClient.send(
                request,
                HttpResponse.BodyHandlers.ofString()
        );

        if (response.statusCode() != 201) {
            throw new RuntimeException(
                    "Search API returned status "
                            + response.statusCode()
                            + ": "
                            + response.body()
            );
        }
    }

    private long parseTimestamp(String timestamp) {

        try {
            // If the gRPC client already sends epoch millis as a string
            return Long.parseLong(timestamp);

        } catch (NumberFormatException e) {

            // Fallback: use current time
            System.err.println(
                    "Could not parse timestamp '"
                            + timestamp
                            + "', using current time instead"
            );

            return System.currentTimeMillis();
        }
    }

    private String escape(String value) {

        if (value == null) {
            return "";
        }

        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}