package com.orderflow.alerting_engine_service.notification;

import com.orderflow.alerting_engine_service.event.AlertFiredEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Stand-in for a real webhook/email integration. Per the project spec this is
 * simulated — logs what would be sent, rather than calling a real external
 * service or SMTP server.
 */
@Component
@Slf4j
public class WebhookNotifier {

    public void notify(AlertFiredEvent event) {
        log.warn("""
                >>> SIMULATED ALERT NOTIFICATION <
                Rule:      {}
                Query:     {}
                Threshold: {}   Actual: {}   Window: {} min
                Fired at:  {}
                (In production this would POST to a webhook URL or send an email)
                """,
                event.getRuleName(), event.getQuery(), event.getThreshold(),
                event.getActualCount(), event.getTimeWindowMinutes(), event.getFiredAt());
    }
}
