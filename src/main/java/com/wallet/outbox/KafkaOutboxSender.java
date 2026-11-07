package com.wallet.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/** Key = account id; headers carry the outbox row id (the consumer's dedupe key) and the event type. */
@Component
public class KafkaOutboxSender implements OutboxSender {

    private final KafkaTemplate<String, String> template;
    private final String topic;
    private final long sendTimeoutMs;

    public KafkaOutboxSender(KafkaTemplate<String, String> template,
                             @Value("${app.outbox.topic}") String topic,
                             @Value("${app.outbox.relay.send-timeout-ms:12000}") long sendTimeoutMs) {
        this.template = template;
        this.topic = topic;
        this.sendTimeoutMs = sendTimeoutMs;
    }

    @Override
    public void send(OutboxRelay.Row row) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, row.aggregateId(), row.payload());
        record.headers().add("event-id", Long.toString(row.id()).getBytes(StandardCharsets.UTF_8));
        record.headers().add("event-type", row.type().getBytes(StandardCharsets.UTF_8));
        template.send(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
    }
}
