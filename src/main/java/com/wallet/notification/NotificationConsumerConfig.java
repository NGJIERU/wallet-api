package com.wallet.notification;

import com.wallet.metrics.EventMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Duration;
import java.util.function.Supplier;

/** Container factory and error policy for the notification consumer. */
@Configuration
public class NotificationConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumerConfig.class);

    @Bean
    DefaultErrorHandler notificationErrorHandler(
            @Value("${app.notifications.retry.attempts:4}") int attempts,
            @Value("${app.notifications.retry.initial-backoff:500ms}") Duration initial,
            @Value("${app.notifications.retry.multiplier:2.0}") double multiplier,
            KafkaTemplate<String, String> template,
            ObjectProvider<EventMetrics> metricsProvider) {
        ExponentialBackOff backOff = new ExponentialBackOff(initial.toMillis(), multiplier);
        backOff.setMaxAttempts(Math.max(0, attempts - 1)); // max attempts counts retries after the first try
        // failure path: publish to <topic>-dlt (same partition, original key/value/headers plus exception headers).
        // If the DLT publish fails, the handler redelivers the record, so nothing is lost. Never log the payload.
        DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(template);
        DefaultErrorHandler handler = new DefaultErrorHandler(deadLetterRecoverer(dlt, metricsProvider::getIfAvailable), backOff);
        handler.addNotRetryableExceptions(PoisonEventException.class);
        handler.setCommitRecovered(true);
        return handler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> notificationListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, DefaultErrorHandler notificationErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> f = new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(consumerFactory);
        f.setConcurrency(1);
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        f.getContainerProperties().setMissingTopicsFatal(false);
        f.setCommonErrorHandler(notificationErrorHandler);
        return f;
    }

    /** Logs, publishes to the DLT, and only if the publish returned normally counts dead_lettered. */
    static ConsumerRecordRecoverer deadLetterRecoverer(ConsumerRecordRecoverer publisher, Supplier<EventMetrics> metrics) {
        return (ConsumerRecord<?, ?> record, Exception e) -> {
            log.error("notification record dead-lettered: topic={} partition={} offset={} eventId={} cause={}",
                    record.topic(), record.partition(), record.offset(),
                    NotificationListener.header(record, "event-id"), rootName(e));
            publisher.accept(record, e);
            EventMetrics m = metrics.get();
            if (m != null) {
                m.consumerEvent("dead_lettered");
            }
        };
    }

    private static String rootName(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName();
    }
}
