package com.orderflow.alerting_engine_service.repository;

import com.orderflow.alerting_engine_service.model.AlertRule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AlertRuleRepository extends JpaRepository<AlertRule, Long> {

    // Used by the Week 3 scheduler — only fetch rules that are turned on
    List<AlertRule> findByEnabledTrue();
}
