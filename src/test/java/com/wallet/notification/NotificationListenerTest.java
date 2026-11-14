package com.wallet.notification;

import com.wallet.AbstractIntegrationTest;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** CT4 and listener classification, through the adapter's real code path (no Kafka read, no sleeps). */
class NotificationListenerTest extends AbstractIntegrationTest {

    @Autowired NotificationListener listener;
    @Autowired UserRepository users;
    @Autowired JdbcClient jdbc;

    // CT4: the relay duplicate shape (the same event-id delivered twice) gives one notification
    @Test
    void theSameEventDeliveredTwiceThroughTheListenerGivesOneNotification() {
        long userId = users.saveAndFlush(new User("listener-" + UUID.randomUUID() + "@example.test", "x")).getId();
        long eventId = ThreadLocalRandom.current().nextLong(1L << 40, 1L << 50);
        String payload = NotificationTestData.payload(UUID.randomUUID(), ThreadLocalRandom.current().nextLong(1L << 40, 1L << 50),
                ThreadLocalRandom.current().nextLong(1L << 40, 1L << 50), userId, "CREDIT", "DEPOSIT", 900, "SGD");

        listener.onMessage(record(eventId, "TransactionCompleted.v1", payload, 0));
        listener.onMessage(record(eventId, "TransactionCompleted.v1", payload, 1));

        assertThat(jdbc.sql("SELECT COUNT(*) FROM notifications WHERE event_id = ?").param(eventId).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT user_id FROM notifications WHERE event_id = ?").param(eventId).query(Long.class).single()).isEqualTo(userId);
    }

    @Test
    void aPoisonRecordRaisesTheNonRetryableException() {
        assertThatThrownBy(() -> listener.onMessage(record(7L, "TransactionCompleted.v1", "not json {", 0)))
                .isInstanceOf(PoisonEventException.class);
    }

    @Test
    void aRecordWithoutTheEventIdHeaderIsPoison() {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("t", 0, 0L, "k", "{}");
        r.headers().add("event-type", "TransactionCompleted.v1".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> listener.onMessage(r)).isInstanceOf(PoisonEventException.class);
    }

    private static ConsumerRecord<String, String> record(long eventId, String type, String payload, long offset) {
        ConsumerRecord<String, String> r = new ConsumerRecord<>("wallet.transaction-completed.v1", 0, offset, "k", payload);
        r.headers().add("event-id", Long.toString(eventId).getBytes(StandardCharsets.UTF_8));
        r.headers().add("event-type", type.getBytes(StandardCharsets.UTF_8));
        return r;
    }
}
