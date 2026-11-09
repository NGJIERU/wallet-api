package com.wallet;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest(properties = {"app.jwt.secret=test-secret-test-secret-test-secret-123456",
        "app.outbox.relay.enabled=false", "app.notifications.consumer.enabled=false"})
@AutoConfigureMockMvc
@ExtendWith(ReconciliationExtension.class)
public abstract class AbstractIntegrationTest {

    // one container for the whole test run, shared by every integration test class
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine")
            .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=200");

    // same Kafka version as docker-compose.yml; auto-creation off so a wrong topic name fails loudly
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.2")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }
}