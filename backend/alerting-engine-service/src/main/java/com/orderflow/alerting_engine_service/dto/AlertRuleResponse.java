package com.orderflow.alerting_engine_service.dto;

import com.orderflow.alerting_engine_service.model.AlertRule;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AlertRuleResponse {

    private Long id;
    private String name;
    private String query;
    private Integer threshold;
    private Integer timeWindowMinutes;
    private boolean enabled;
    private LocalDateTime createdAt;
    private LocalDateTime lastTriggeredAt;

    public static AlertRuleResponse fromEntity(AlertRule rule) {
        AlertRuleResponse response = new AlertRuleResponse();
        response.setId(rule.getId());
        response.setName(rule.getName());
        response.setQuery(rule.getQuery());
        response.setThreshold(rule.getThreshold());
        response.setTimeWindowMinutes(rule.getTimeWindowMinutes());
        response.setEnabled(rule.isEnabled());
        response.setCreatedAt(rule.getCreatedAt());
        response.setLastTriggeredAt(rule.getLastTriggeredAt());
        return response;
    }
}
