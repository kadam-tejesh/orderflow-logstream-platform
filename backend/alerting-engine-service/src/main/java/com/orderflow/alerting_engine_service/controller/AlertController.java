package com.orderflow.alerting_engine_service.controller;

import com.orderflow.alerting_engine_service.dto.AlertRuleRequest;
import com.orderflow.alerting_engine_service.dto.AlertRuleResponse;
import com.orderflow.alerting_engine_service.service.AlertService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/alerts")
@RequiredArgsConstructor
public class AlertController {

    private final AlertService alertService;

    @PostMapping
    public ResponseEntity<AlertRuleResponse> createRule(@Valid @RequestBody AlertRuleRequest request) {
        AlertRuleResponse response = alertService.createRule(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping
    public ResponseEntity<List<AlertRuleResponse>> getAllRules() {
        return ResponseEntity.ok(alertService.getAllRules());
    }

    @GetMapping("/{id}")
    public ResponseEntity<AlertRuleResponse> getRuleById(@PathVariable Long id) {
        return ResponseEntity.ok(alertService.getRuleById(id));
    }

    @PutMapping("/{id}")
    public ResponseEntity<AlertRuleResponse> updateRule(@PathVariable Long id,
                                                        @Valid @RequestBody AlertRuleRequest request) {
        return ResponseEntity.ok(alertService.updateRule(id, request));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteRule(@PathVariable Long id) {
        alertService.deleteRule(id);
        return ResponseEntity.noContent().build();
    }
}
