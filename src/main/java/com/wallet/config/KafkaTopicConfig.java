package com.wallet.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Declares the events topic so the contract lives in code. Key = account id, so one account's
 * events always land in one partition (per-account order). Single broker, so replication factor 1.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String TRANSACTION_COMPLETED_TOPIC = "wallet.transaction-completed.v1";

    @Bean
    NewTopic transactionCompletedTopic(@Value("${app.outbox.topic:" + TRANSACTION_COMPLETED_TOPIC + "}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    /**
     * Dead-letter topic. Spring Kafka's default recoverer publishes to <topic>-dlt on the SAME partition number,
     * so it needs the same partition count as the main topic. Declared through NewTopics (not a second NewTopic bean)
     * so the single NewTopic injected by the relay stays unambiguous.
     */
    @Bean
    KafkaAdmin.NewTopics deadLetterTopics(@Value("${app.outbox.topic:" + TRANSACTION_COMPLETED_TOPIC + "}") String topic) {
        return new KafkaAdmin.NewTopics(TopicBuilder.name(topic + "-dlt").partitions(3).replicas(1).build());
    }
}
