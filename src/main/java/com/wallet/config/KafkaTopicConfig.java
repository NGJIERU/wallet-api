package com.wallet.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the events topic so the contract lives in code. Key = account id, so one account's
 * events always land in one partition (per-account order). Single broker, so replication factor 1.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String TRANSACTION_COMPLETED_TOPIC = "wallet.transaction-completed.v1";

    @Bean
    NewTopic transactionCompletedTopic() {
        return TopicBuilder.name(TRANSACTION_COMPLETED_TOPIC).partitions(3).replicas(1).build();
    }
}
