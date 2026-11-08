package com.wallet.outbox;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/** Key = account id; headers carry the outbox row id (the consumer's dedupe key) and the event type. */
@Component
public class KafkaOutboxSender implements OutboxSender {

    private final KafkaTemplate<String, String> template;
    private final String topic;
    private final long sendTimeoutMs;
    private final KafkaAdmin kafkaAdmin;
    private final NewTopic topicDeclaration;

    public KafkaOutboxSender(KafkaTemplate<String, String> template,
                             @Value("${app.outbox.topic}") String topic,
                             @Value("${app.outbox.relay.send-timeout-ms:12000}") long sendTimeoutMs,
                             KafkaAdmin kafkaAdmin, NewTopic topicDeclaration) {
        this.template = template;
        // the default listener logs the failed record (payload with account ids and amounts) at ERROR;
        // the relay already logs a WARN with counts and the cause, so silence the producer-side log
        template.setProducerListener(new ProducerListener<>() {
        });
        this.topic = topic;
        this.sendTimeoutMs = sendTimeoutMs;
        this.kafkaAdmin = kafkaAdmin;
        this.topicDeclaration = topicDeclaration;
    }

    @Override
    public void send(OutboxRelay.Row row) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, row.aggregateId(), row.payload());
        record.headers().add("event-id", Long.toString(row.id()).getBytes(StandardCharsets.UTF_8));
        record.headers().add("event-type", row.type().getBytes(StandardCharsets.UTF_8));
        template.send(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Idempotent: creates the topic only if it is missing (broker auto-creation is off on purpose). */
    @Override
    public void prepareRetry() {
        kafkaAdmin.createOrModifyTopics(topicDeclaration);
    }
}
