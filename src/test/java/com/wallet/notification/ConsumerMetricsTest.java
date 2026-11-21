package com.wallet.notification;

import com.wallet.AbstractIntegrationTest;
import com.wallet.metrics.EventMetrics;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** RC4: consumer outcome counters, each test with its own registry (the consumer itself is off in tests). */
class ConsumerMetricsTest extends AbstractIntegrationTest {

    private static final String TYPE = "TransactionCompleted.v1";

    @Autowired NotificationHandler handler;
    @Autowired UserRepository users;

    private static double outcome(MeterRegistry reg, String o) {
        return reg.get("wallet.consumer.events").tag("outcome", o).counter().count();
    }

    private static long newId() {
        return ThreadLocalRandom.current().nextLong(1L << 40, 1L << 50);
    }

    private static ConsumerRecord<String, String> record(long eventId, String type, String payload, long offset) {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("wallet.transaction-completed.v1", 0, offset, "k", payload);
        r.headers().add("event-id", Long.toString(eventId).getBytes(StandardCharsets.UTF_8));
        r.headers().add("event-type", type.getBytes(StandardCharsets.UTF_8));
        return r;
    }

    @Test
    void rc4_processedDuplicateAndSkippedAreCountedAndAPoisonRecordCountsNothingHere() {
        MeterRegistry reg = new SimpleMeterRegistry();
        NotificationListener listener = new NotificationListener(handler);
        listener.setMetrics(new EventMetrics(reg));
        for (String o : EventMetrics.CONSUMER_OUTCOMES) {
            assertThat(outcome(reg, o)).as("exists at 0: " + o).isZero();
        }
        long userId = users.saveAndFlush(new User("cm-" + UUID.randomUUID() + "@example.test", "x")).getId();
        long eventId = newId();
        String payload = NotificationTestData.payload(UUID.randomUUID(), newId(), newId(), userId, "CREDIT", "DEPOSIT", 900, "SGD");

        listener.onMessage(record(eventId, TYPE, payload, 0));
        assertThat(outcome(reg, "processed")).isEqualTo(1.0);
        listener.onMessage(record(eventId, TYPE, payload, 1));
        assertThat(outcome(reg, "duplicate")).isEqualTo(1.0);
        listener.onMessage(record(newId(), "SomethingElse.v9", payload, 2));
        assertThat(outcome(reg, "skipped")).isEqualTo(1.0);

        assertThatThrownBy(() -> listener.onMessage(record(newId(), TYPE, "not json {", 3)))
                .isInstanceOf(PoisonEventException.class);
        assertThat(outcome(reg, "processed")).isEqualTo(1.0);
        assertThat(outcome(reg, "duplicate")).isEqualTo(1.0);
        assertThat(outcome(reg, "skipped")).isEqualTo(1.0);
        assertThat(outcome(reg, "dead_lettered")).as("only the recoverer counts dead letters").isZero();
    }

    @Test
    void rc4_deadLetteredIsCountedOnlyAfterTheDltPublishSucceeds() {
        MeterRegistry reg = new SimpleMeterRegistry();
        EventMetrics metrics = new EventMetrics(reg);
        ConsumerRecord<String, String> r = record(newId(), TYPE, "x", 0);
        Exception cause = new PoisonEventException("poison");

        NotificationConsumerConfig.deadLetterRecoverer((rec, ex) -> { }, () -> metrics).accept(r, cause);
        assertThat(outcome(reg, "dead_lettered")).isEqualTo(1.0);

        assertThatThrownBy(() -> NotificationConsumerConfig.deadLetterRecoverer((rec, ex) -> {
            throw new IllegalStateException("dlt publish failed");
        }, () -> metrics).accept(r, cause)).isInstanceOf(IllegalStateException.class);
        assertThat(outcome(reg, "dead_lettered")).as("a failed DLT publish is not counted").isEqualTo(1.0);
    }
}
