package com.wallet.notification;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Duration;

/** Container factory and error policy for the notification consumer. */
@Configuration
public class NotificationConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumerConfig.class);

    @Bean
    DefaultErrorHandler notificationErrorHandler(
            @Value("${app.notifications.retry.attempts:4}") int attempts,
            @Value("${app.notifications.retry.initial-backoff:500ms}") Duration initial,
            @Value("${app.notifications.retry.multiplier:2.0}") double multiplier) {
        ExponentialBackOff backOff = new ExponentialBackOff(initial.toMillis(), multiplier);
        backOff.setMaxAttempts(Math.max(0, attempts - 1)); // max attempts counts retries after the first try
        // failure path for now: log and skip. Never the payload, never the exception message.
        DefaultErrorHandler handler = new DefaultErrorHandler((ConsumerRecord<?, ?> record, Exception e) ->
                log.error("notification record skipped: topic={} partition={} offset={} eventId={} cause={}",
                        record.topic(), record.partition(), record.offset(),
                        NotificationListener.header(record, "event-id"), rootName(e)), backOff);
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

    private static String rootName(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName();
    }
}
