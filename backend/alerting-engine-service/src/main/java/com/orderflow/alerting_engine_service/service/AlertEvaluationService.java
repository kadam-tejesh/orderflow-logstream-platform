package com.orderflow.alerting_engine_service.service;

import com.orderflow.alerting_engine_service.client.SearchApiClient;
import com.orderflow.alerting_engine_service.model.AlertRule;
import com.orderflow.alerting_engine_service.repository.AlertRuleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class AlertEvaluationService {

    private final AlertRuleRepository alertRuleRepository;
    private final SearchApiClient searchApiClient;

    public void evaluateAllRules() {
        List<AlertRule> enabledRules = alertRuleRepository.findByEnabledTrue();
        log.info("Evaluating {} enabled alert rule(s)", enabledRules.size());

        for (AlertRule rule : enabledRules) {
            evaluateRule(rule);
        }
    }

    private void evaluateRule(AlertRule rule) {
        String windowedQuery = buildWindowedQuery(rule);

        try {
            long matchCount = searchApiClient.countMatches(windowedQuery);
            boolean breached = matchCount > rule.getThreshold();

            log.info("Rule '{}': {} matches in last {} min (threshold {}) -> {}",
                    rule.getName(), matchCount, rule.getTimeWindowMinutes(),
                    rule.getThreshold(), breached ? "BREACHED" : "ok");

            if (breached) {
                // Week 4: fire webhook/email + publish Kafka event here
            }
        } catch (Exception ex) {
            log.error("Failed to evaluate rule '{}': {}", rule.getName(), ex.getMessage());
        }
    }

    private String buildWindowedQuery(AlertRule rule) {
        long now = System.currentTimeMillis();
        long windowStart = now - (rule.getTimeWindowMinutes() * 60_000L);
        return "(" + rule.getQuery() + ") AND timestamp:[" + windowStart + " TO " + now + "]";
    }
}
