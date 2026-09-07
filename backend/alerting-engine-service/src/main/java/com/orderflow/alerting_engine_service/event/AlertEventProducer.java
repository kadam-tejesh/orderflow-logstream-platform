package com.orderflow.alerting_engine_service.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AlertEventProducer {

    private final KafkaTemplate<String, AlertFiredEvent> kafkaTemplate;

    @Value("${alerting.kafka.topic}")
    private String topic;

    public void publishAlertFired(AlertFiredEvent event) {
        kafkaTemplate.send(topic, event.getRuleId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish alert-fired event for rule {}: {}",
                                event.getRuleName(), ex.getMessage());
                    } else {
                        log.info("Published alert-fired event for rule '{}' to topic '{}'",
                                event.getRuleName(), topic);
                    }
                });
    }
}