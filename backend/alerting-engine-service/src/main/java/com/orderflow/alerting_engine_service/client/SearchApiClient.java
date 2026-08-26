package com.orderflow.alerting_engine_service.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class SearchApiClient {

    private final RestClient restClient;

    public SearchApiClient(@Value("${search.api.base-url}") String baseUrl) {
        this.restClient = RestClient.builder().baseUrl(baseUrl).build();
    }

    /**
     * Calls GET /api/search on the Search & Indexing Engine and returns totalHits.
     * Query is built by AlertEvaluationService to include the rule's condition
     * AND a timestamp range for the rule's time window.
     */
    public long countMatches(String query) {
        SearchCountResponse response = restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/api/search").queryParam("q", query).build())
                .retrieve()
                .body(SearchCountResponse.class);

        return response != null ? response.totalHits() : 0L;
    }

    private record SearchCountResponse(long totalHits, Object results) {
    }
}
