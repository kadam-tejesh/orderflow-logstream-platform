package com.orderflow.alerting_engine_service.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AlertRuleRequest {

    @NotBlank(message = "name is required")
    private String name;

    @NotBlank(message = "query is required")
    private String query;

    @NotNull(message = "threshold is required")
    @Min(value = 1, message = "threshold must be at least 1")
    private Integer threshold;

    @NotNull(message = "timeWindowMinutes is required")
    @Min(value = 1, message = "timeWindowMinutes must be at least 1")
    private Integer timeWindowMinutes;

    private Boolean enabled;
}