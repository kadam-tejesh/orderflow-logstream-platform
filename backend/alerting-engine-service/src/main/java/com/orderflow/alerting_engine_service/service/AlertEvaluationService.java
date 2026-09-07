package com.orderflow.alerting_engine_service.service;

import com.orderflow.alerting_engine_service.client.SearchApiClient;
import com.orderflow.alerting_engine_service.event.AlertEventProducer;
import com.orderflow.alerting_engine_service.event.AlertFiredEvent;
import com.orderflow.alerting_engine_service.model.AlertRule;
import com.orderflow.alerting_engine_service.notification.WebhookNotifier;
import com.orderflow.alerting_engine_service.repository.AlertRuleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class AlertEvaluationService {

    private final AlertRuleRepository alertRuleRepository;
    private final SearchApiClient searchApiClient;
    private final WebhookNotifier webhookNotifier;
    private final AlertEventProducer alertEventProducer;

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
                handleBreach(rule, matchCount);
            }
        } catch (Exception ex) {
            log.error("Failed to evaluate rule '{}': {}", rule.getName(), ex.getMessage());
        }
    }

    private void handleBreach(AlertRule rule, long matchCount) {
        AlertFiredEvent event = new AlertFiredEvent(
                rule.getId(),
                rule.getName(),
                rule.getQuery(),
                rule.getThreshold(),
                matchCount,
                rule.getTimeWindowMinutes(),
                System.currentTimeMillis()
        );

        webhookNotifier.notify(event);
        alertEventProducer.publishAlertFired(event);

        rule.setLastTriggeredAt(LocalDateTime.now());
        alertRuleRepository.save(rule);
    }

    private String buildWindowedQuery(AlertRule rule) {
        long now = System.currentTimeMillis();
        long windowStart = now - (rule.getTimeWindowMinutes() * 60_000L);
        return "(" + rule.getQuery() + ") AND timestamp:[" + windowStart + " TO " + now + "]";
    }
}
