package com.wallet.kafka;

import com.wallet.AbstractIntegrationTest;
import com.wallet.config.KafkaTopicConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Infrastructure check only: the test broker works, the topic has the agreed shape, keys keep order. */
class KafkaInfrastructureTest extends AbstractIntegrationTest {

    private static final String TOPIC = KafkaTopicConfig.TRANSACTION_COMPLETED_TOPIC;

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrap;

    @Test
    void topicHasThreePartitionsAndReplicationFactorOne() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            TopicDescription d = admin.describeTopics(List.of(TOPIC)).allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC);
            assertThat(d.partitions()).hasSize(3);
            assertThat(d.partitions()).allSatisfy(p -> assertThat(p.replicas()).hasSize(1));
        }
    }

    @Test
    void sameKeyKeepsOrderOnOnePartitionAndAGroupConsumerReceivesIt() throws Exception {
        String key = "acct-" + UUID.randomUUID();
        int n = 5;
        Set<Integer> keyPartitions = new HashSet<>();
        Set<Integer> otherPartitions = new HashSet<>();
        long lastOffset = -1;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            for (int i = 0; i < n; i++) {
                RecordMetadata m = producer.send(new ProducerRecord<>(TOPIC, key, String.valueOf(i))).get(30, TimeUnit.SECONDS);
                keyPartitions.add(m.partition());
                assertThat(m.offset()).isGreaterThan(lastOffset);
                lastOffset = m.offset();
            }
            // different keys can land elsewhere: across 30 keys more than one partition must be used
            for (int k = 0; k < 30; k++) {
                otherPartitions.add(producer.send(new ProducerRecord<>(TOPIC, "other-" + UUID.randomUUID(), "x"))
                        .get(30, TimeUnit.SECONDS).partition());
            }
        }
        assertThat(keyPartitions).as("one key, one partition").hasSize(1);
        assertThat(otherPartitions).as("different keys spread over partitions").hasSizeGreaterThan(1);

        List<String> received = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "infra-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(TOPIC)); // group subscription, not manual assignment
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (received.size() < n && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(r.key())) {
                        received.add(r.value());
                    }
                }
            }
        }
        assertThat(received).containsExactly("0", "1", "2", "3", "4");
    }
}
