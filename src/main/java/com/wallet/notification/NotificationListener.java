package com.wallet.notification;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Thin adapter: reads the event-id and event-type headers and hands the record to the handler.
 * Starts only when app.notifications.consumer.enabled=true (off in tests, which drive the handler directly).
 * The offset is committed by the container AFTER this method returns, i.e. after the handler's DB commit.
 */
@Component
public class NotificationListener {

    private final NotificationHandler handler;

    public NotificationListener(NotificationHandler handler) {
        this.handler = handler;
    }

    public static final String LISTENER_ID = "notification-listener";

    // autoStartup=false on purpose: the container is started by NotificationConsumerLifecycle on its own thread,
    // with retry, because constructing the Kafka consumer fails when the broker address does not resolve
    // and that must never abort application start-up.
    @KafkaListener(id = LISTENER_ID, topics = "${app.outbox.topic}",
            groupId = "${app.notifications.consumer-group:wallet-notifications}",
            containerFactory = "notificationListenerContainerFactory",
            autoStartup = "false")
    public void onMessage(ConsumerRecord<String, String> record) {
        handler.handle(header(record, "event-id"), header(record, "event-type"), record.value());
    }

    static String header(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
