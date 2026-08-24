package com.orderflow.alerting_engine_service.exception;

public class AlertRuleNotFoundException extends RuntimeException {
    public AlertRuleNotFoundException(Long id) {
        super("Alert rule not found with id: " + id);
    }
}
