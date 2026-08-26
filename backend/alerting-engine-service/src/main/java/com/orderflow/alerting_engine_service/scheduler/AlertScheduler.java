package com.orderflow.alerting_engine_service.scheduler;

import com.orderflow.alerting_engine_service.service.AlertEvaluationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AlertScheduler {

    private final AlertEvaluationService alertEvaluationService;

    @Scheduled(fixedRateString = "${alerting.scheduler.fixed-rate-ms}")
    public void runScheduledEvaluation() {
        alertEvaluationService.evaluateAllRules();
    }
}