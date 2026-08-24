package com.orderflow.alerting_engine_service.service;

import com.orderflow.alerting_engine_service.dto.AlertRuleRequest;
import com.orderflow.alerting_engine_service.dto.AlertRuleResponse;
import com.orderflow.alerting_engine_service.exception.AlertRuleNotFoundException;
import com.orderflow.alerting_engine_service.model.AlertRule;
import com.orderflow.alerting_engine_service.repository.AlertRuleRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class AlertService {

    private final AlertRuleRepository alertRuleRepository;

    public AlertRuleResponse createRule(AlertRuleRequest request) {
        AlertRule rule = AlertRule.builder()
                .name(request.getName())
                .query(request.getQuery())
                .threshold(request.getThreshold())
                .timeWindowMinutes(request.getTimeWindowMinutes())
                .enabled(request.getEnabled() == null || request.getEnabled())
                .build();

        return AlertRuleResponse.fromEntity(alertRuleRepository.save(rule));
    }

    public List<AlertRuleResponse> getAllRules() {
        return alertRuleRepository.findAll()
                .stream()
                .map(AlertRuleResponse::fromEntity)
                .toList();
    }

    public AlertRuleResponse getRuleById(Long id) {
        AlertRule rule = alertRuleRepository.findById(id)
                .orElseThrow(() -> new AlertRuleNotFoundException(id));
        return AlertRuleResponse.fromEntity(rule);
    }

    public AlertRuleResponse updateRule(Long id, AlertRuleRequest request) {
        AlertRule rule = alertRuleRepository.findById(id)
                .orElseThrow(() -> new AlertRuleNotFoundException(id));

        rule.setName(request.getName());
        rule.setQuery(request.getQuery());
        rule.setThreshold(request.getThreshold());
        rule.setTimeWindowMinutes(request.getTimeWindowMinutes());
        if (request.getEnabled() != null) {
            rule.setEnabled(request.getEnabled());
        }

        return AlertRuleResponse.fromEntity(alertRuleRepository.save(rule));
    }

    public void deleteRule(Long id) {
        if (!alertRuleRepository.existsById(id)) {
            throw new AlertRuleNotFoundException(id);
        }
        alertRuleRepository.deleteById(id);
    }
}
