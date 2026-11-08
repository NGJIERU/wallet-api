package com.wallet.notification;

import com.wallet.AbstractIntegrationTest;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** CT1, CT2, CT3, CT7: the handler against a real database, scoped to the ids each test creates. */
class NotificationHandlerTest extends AbstractIntegrationTest {

    private static final String TYPE = "TransactionCompleted.v1";

    @Autowired NotificationHandler handler;
    @Autowired UserRepository users;
    @Autowired JdbcClient jdbc;
    @Value("${app.notifications.consumer-group:wallet-notifications}") String group;

    // CT1
    @Test
    void oneEventWritesOneMarkerAndOneCorrectNotificationAndTheConstraintsHold() {
        long userId = user();
        long eventId = newId();
        UUID txn = UUID.randomUUID();
        long ledgerId = newId();
        long accountId = newId();

        var result = handler.handle(Long.toString(eventId), TYPE,
                NotificationTestData.payload(txn, ledgerId, accountId, userId, "CREDIT", "DEPOSIT", 2500, "SGD"));

        assertThat(result).isEqualTo(NotificationHandler.Result.CREATED);
        assertThat(markers(eventId)).isEqualTo(1);
        assertThat(notifications(eventId)).isEqualTo(1);
        assertThat(col(eventId, "user_id", Long.class)).isEqualTo(userId);
        assertThat(col(eventId, "account_id", Long.class)).isEqualTo(accountId);
        assertThat(col(eventId, "transaction_id", UUID.class)).isEqualTo(txn);
        assertThat(col(eventId, "ledger_entry_id", Long.class)).isEqualTo(ledgerId);
        assertThat(col(eventId, "direction", String.class)).isEqualTo("CREDIT");
        assertThat(col(eventId, "transaction_type", String.class)).isEqualTo("DEPOSIT");
        assertThat(col(eventId, "amount", Long.class)).isEqualTo(2500);
        assertThat(col(eventId, "currency", String.class)).isEqualTo("SGD");
        assertThat(col(eventId, "message", String.class)).isEqualTo("Received SGD 25.00 (deposit)");
        assertThat(col(eventId, "occurred_at", OffsetDateTime.class).toInstant())
                .isEqualTo(Instant.parse(NotificationTestData.OCCURRED_AT));

        assertThatThrownBy(() -> insertRaw(eventId, userId, "CREDIT", 100)).as("unique event_id")
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> insertRaw(newId(), userId, "SIDEWAYS", 100)).as("direction check")
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> insertRaw(newId(), userId, "CREDIT", 0)).as("amount check")
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    // CT2
    @Test
    void theSameEventHandledTwiceGivesOneNotification() {
        long userId = user();
        long eventId = newId();
        String payload = NotificationTestData.payload(UUID.randomUUID(), newId(), newId(), userId, "DEBIT", "TRANSFER", 1000, "SGD");

        assertThat(handler.handle(Long.toString(eventId), TYPE, payload)).isEqualTo(NotificationHandler.Result.CREATED);
        assertThat(handler.handle(Long.toString(eventId), TYPE, payload)).isEqualTo(NotificationHandler.Result.DUPLICATE);

        assertThat(notifications(eventId)).isEqualTo(1);
        assertThat(markers(eventId)).isEqualTo(1);
    }

    // CT3
    @Test
    void aFailingNotificationInsertLeavesNoMarkerAndTheSameEventThenSucceedsOnce() {
        long userId = user();
        long eventId = newId();
        String payload = NotificationTestData.payload(UUID.randomUUID(), newId(), newId(), userId, "CREDIT", "DEPOSIT", 700, "SGD");

        installFailingTrigger(userId);
        try {
            assertThatThrownBy(() -> handler.handle(Long.toString(eventId), TYPE, payload))
                    .hasStackTraceContaining("injected notification failure")
                    .isNotInstanceOf(PoisonEventException.class);
            assertThat(markers(eventId)).as("marker rolled back with the failed insert").isZero();
            assertThat(notifications(eventId)).isZero();
        } finally {
            dropFailingTrigger();
        }

        assertThat(handler.handle(Long.toString(eventId), TYPE, payload)).isEqualTo(NotificationHandler.Result.CREATED);
        assertThat(markers(eventId)).isEqualTo(1);
        assertThat(notifications(eventId)).isEqualTo(1);
    }

    // CT7
    @Test
    void anExtraPayloadFieldIsIgnored() {
        long userId = user();
        long eventId = newId();
        String base = NotificationTestData.payload(UUID.randomUUID(), newId(), newId(), userId, "CREDIT", "DEPOSIT", 100, "SGD");
        String withExtra = base.substring(0, base.length() - 1) + ",\"futureField\":{\"x\":1}}";

        assertThat(handler.handle(Long.toString(eventId), TYPE, withExtra)).isEqualTo(NotificationHandler.Result.CREATED);
        assertThat(notifications(eventId)).isEqualTo(1);
    }

    // CT7
    @Test
    void malformedPayloadsArePoisonAndLeaveNoRows() {
        long userId = user();
        String good = NotificationTestData.payload(UUID.randomUUID(), newId(), newId(), userId, "CREDIT", "DEPOSIT", 100, "SGD");
        String[] bad = {
                "not json {",
                null,
                "{}",
                "[]",
                good.replace("\"ownerUserId\":" + userId + ",", ""),
                good.replace("\"amount\":100", "\"amount\":\"100\""),
                good.replace("\"amount\":100", "\"amount\":1.5"),
                good.replace("\"amount\":100", "\"amount\":0"),
                good.replace("\"direction\":\"CREDIT\"", "\"direction\":\"SIDEWAYS\""),
                good.replace("\"currency\":\"SGD\"", "\"currency\":\"sgd\""),
                good.replaceFirst("\"transactionId\":\"[^\"]+\"", "\"transactionId\":\"nope\""),
                good.replace("2026-11-09T01:02:03.456789Z", "yesterday")
        };
        for (String p : bad) {
            long eventId = newId();
            assertThatThrownBy(() -> handler.handle(Long.toString(eventId), TYPE, p))
                    .as("payload: " + (p == null ? "null" : p.length() > 40 ? p.substring(0, 40) : p))
                    .isInstanceOf(PoisonEventException.class);
            assertThat(markers(eventId)).isZero();
            assertThat(notifications(eventId)).isZero();
        }
    }

    // CT7
    @Test
    void aMissingOrNonNumericEventIdHeaderIsPoison() {
        long userId = user();
        String good = NotificationTestData.payload(UUID.randomUUID(), newId(), newId(), userId, "CREDIT", "DEPOSIT", 100, "SGD");
        for (String header : new String[]{null, "", "abc", "12.5"}) {
            assertThatThrownBy(() -> handler.handle(header, TYPE, good))
                    .as("header: " + header)
                    .isInstanceOf(PoisonEventException.class);
        }
    }

    // CT7
    @Test
    void anUnknownEventTypeIsSkippedWithNoRows() {
        long userId = user();
        long eventId = newId();
        String good = NotificationTestData.payload(UUID.randomUUID(), newId(), newId(), userId, "CREDIT", "DEPOSIT", 100, "SGD");

        assertThat(handler.handle(Long.toString(eventId), "SomethingElse.v9", good))
                .isEqualTo(NotificationHandler.Result.SKIPPED_UNKNOWN_TYPE);
        assertThat(handler.handle(Long.toString(eventId), null, good))
                .isEqualTo(NotificationHandler.Result.SKIPPED_UNKNOWN_TYPE);

        assertThat(markers(eventId)).isZero();
        assertThat(notifications(eventId)).isZero();
    }

    private long user() {
        return users.saveAndFlush(new User("notif-" + UUID.randomUUID() + "@example.test", "x")).getId();
    }

    private static long newId() {
        return ThreadLocalRandom.current().nextLong(1L << 40, 1L << 50);
    }

    private long markers(long eventId) {
        return jdbc.sql("SELECT COUNT(*) FROM processed_events WHERE consumer_group = ? AND event_id = ?")
                .param(group).param(eventId).query(Long.class).single();
    }

    private long notifications(long eventId) {
        return jdbc.sql("SELECT COUNT(*) FROM notifications WHERE event_id = ?").param(eventId).query(Long.class).single();
    }

    private <T> T col(long eventId, String column, Class<T> type) {
        return jdbc.sql("SELECT " + column + " FROM notifications WHERE event_id = ?").param(eventId).query(type).single();
    }

    private void insertRaw(long eventId, long userId, String direction, long amount) {
        jdbc.sql("""
                        INSERT INTO notifications (event_id, user_id, account_id, transaction_id, ledger_entry_id,
                                                   transaction_type, direction, amount, currency, message, occurred_at)
                        VALUES (?, ?, 1, ?, 1, 'DEPOSIT', ?, ?, 'SGD', 'raw', now())
                        """)
                .param(eventId).param(userId).param(UUID.randomUUID()).param(direction).param(amount).update();
    }

    private void installFailingTrigger(long userId) {
        jdbc.sql("""
                CREATE OR REPLACE FUNCTION notif_fail_ct3() RETURNS trigger AS $$
                BEGIN
                    IF NEW.user_id = %d THEN
                        RAISE EXCEPTION 'injected notification failure';
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
                """.formatted(userId)).update();
        jdbc.sql("CREATE TRIGGER notif_fail_ct3 BEFORE INSERT ON notifications FOR EACH ROW EXECUTE FUNCTION notif_fail_ct3()").update();
    }

    private void dropFailingTrigger() {
        jdbc.sql("DROP TRIGGER IF EXISTS notif_fail_ct3 ON notifications").update();
        jdbc.sql("DROP FUNCTION IF EXISTS notif_fail_ct3()").update();
    }
}
