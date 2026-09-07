package com.orderflow.alerting_engine_service.event;



import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AlertFiredEvent {
    private Long ruleId;
    private String ruleName;
    private String query;
    private int threshold;
    private long actualCount;
    private int timeWindowMinutes;
    private long firedAt;
}
