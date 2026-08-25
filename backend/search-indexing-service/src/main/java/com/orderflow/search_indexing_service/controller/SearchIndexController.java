package com.orderflow.search_indexing_service.controller;

import com.orderflow.search_indexing_service.dto.LogEntryRequest;
import com.orderflow.search_indexing_service.dto.SearchResultResponse;
import com.orderflow.search_indexing_service.service.IndexingService;
import com.orderflow.search_indexing_service.service.SearchService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class SearchIndexController {

    private final IndexingService indexingService;
    private final SearchService searchService;

    // Stand-in ingestion endpoint until Rajasri's gRPC pipeline feeds this directly.
    @PostMapping("/index")
    public ResponseEntity<Void> indexLog(@Valid @RequestBody LogEntryRequest logEntry) throws Exception {
        indexingService.indexLog(logEntry);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    // e.g. GET /api/search?q=level:ERROR AND service:billing-api
    @GetMapping("/search")
    public ResponseEntity<SearchResultResponse> search(@RequestParam("q") String query) throws Exception {
        return ResponseEntity.ok(searchService.search(query));
    }
}
