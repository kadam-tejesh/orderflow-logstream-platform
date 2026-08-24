package com.orderflow.alerting_engine_service.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "alert_rules")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AlertRule {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    // Lucene query string to run against the Search API,
    // e.g. "level:ERROR AND service:billing-api"
    @Column(nullable = false, length = 1000)
    private String query;

    // Number of matching log entries that must occur within
    // the time window for this rule to be considered "breached"
    @Column(nullable = false)
    private Integer threshold;

    // Rolling window, in minutes, the threshold is evaluated over
    @Column(name = "time_window_minutes", nullable = false)
    private Integer timeWindowMinutes;

    @Column(nullable = false)
    @Builder.Default
    private boolean enabled = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "last_triggered_at")
    private LocalDateTime lastTriggeredAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
    }
}
