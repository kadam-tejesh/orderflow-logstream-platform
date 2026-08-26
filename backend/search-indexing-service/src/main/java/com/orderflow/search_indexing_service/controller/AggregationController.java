package com.orderflow.search_indexing_service.controller;

import com.orderflow.search_indexing_service.service.AggregationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class AggregationController {

    private final AggregationService aggregationService;

    // GET /api/aggregate/minute?rangeMinutes=60
    // Returns log count per minute for the last N minutes — powers the dashboard histogram.
    @GetMapping("/aggregate/minute")
    public Map<Long, Long> countPerMinute(@RequestParam(defaultValue = "60") int rangeMinutes) throws Exception {
        return aggregationService.countPerMinuteBucket(rangeMinutes);
    }
}
