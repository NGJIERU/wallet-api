package com.wallet.outbox;

import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.account.InsufficientFundsException;
import com.wallet.idempotency.IdempotencyKeyReuseException;
import com.wallet.idempotency.StoredResponse;
import com.wallet.transaction.MoneyService;
import com.wallet.transaction.TransferService;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** T1 to T6: outbox events written by the services, scoped to the accounts each test creates. */
class OutboxServiceTest extends AbstractIntegrationTest {

    private static final String TYPE = "TransactionCompleted.v1";

    @Autowired MoneyService money;
    @Autowired TransferService transfers;
    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;

    private record Wallet(long userId, long accountId) {
    }

    private record Event(long id, String aggregateId, String type, boolean unpublished, JsonNode payload) {
    }

    // T1
    @Test
    void depositWritesExactlyOneCorrectEvent() {
        Wallet w = wallet();

        StoredResponse r = money.deposit(w.userId(), w.accountId(), key(), 1_000);

        UUID txnId = txnId(r);
        List<Event> events = eventsFor(w);
        assertThat(events).hasSize(1);
        Event e = events.get(0);
        assertThat(e.type()).isEqualTo(TYPE);
        assertThat(e.aggregateId()).isEqualTo(String.valueOf(w.accountId()));
        assertThat(e.unpublished()).isTrue();
        JsonNode p = e.payload();
        assertThat(p.get("transactionId").asString()).isEqualTo(txnId.toString());
        assertThat(p.get("ledgerEntryId").asLong()).isEqualTo(ledgerEntryId(txnId, w.accountId(), "CREDIT"));
        assertThat(p.get("accountId").asLong()).isEqualTo(w.accountId());
        assertThat(p.get("ownerUserId").asLong()).isEqualTo(w.userId());
        assertThat(p.get("direction").asString()).isEqualTo("CREDIT");
        assertThat(p.get("transactionType").asString()).isEqualTo("DEPOSIT");
        assertThat(p.get("amount").asLong()).isEqualTo(1_000);
        assertThat(p.get("currency").asString()).isEqualTo("SGD");
        assertThat(payloadKeys(e.id())).containsExactlyInAnyOrder(
                "transactionId", "ledgerEntryId", "accountId", "ownerUserId", "direction",
                "transactionType", "amount", "currency", "occurredAt");
        assertReconciled(jdbc, w.accountId());
    }

    // T2
    @Test
    void withdrawalWritesExactlyOneDebitEvent() {
        Wallet w = wallet();
        money.deposit(w.userId(), w.accountId(), key(), 1_000);

        StoredResponse r = money.withdraw(w.userId(), w.accountId(), key(), 400);

        UUID txnId = txnId(r);
        List<Event> events = eventsFor(w);
        assertThat(events).hasSize(2); // the funding deposit + this withdrawal
        Event e = events.get(1);
        assertThat(e.type()).isEqualTo(TYPE);
        assertThat(e.unpublished()).isTrue();
        assertThat(e.payload().get("transactionId").asString()).isEqualTo(txnId.toString());
        assertThat(e.payload().get("direction").asString()).isEqualTo("DEBIT");
        assertThat(e.payload().get("transactionType").asString()).isEqualTo("WITHDRAWAL");
        assertThat(e.payload().get("amount").asLong()).isEqualTo(400);
        assertThat(e.payload().get("ledgerEntryId").asLong()).isEqualTo(ledgerEntryId(txnId, w.accountId(), "DEBIT"));
        assertThat(eventsForTxn(txnId)).hasSize(1);
        assertReconciled(jdbc, w.accountId());
    }

    // T3
    @Test
    void transferWritesADebitEventForTheSenderAndACreditEventForTheReceiver() {
        Wallet alice = wallet();
        Wallet bob = wallet();
        money.deposit(alice.userId(), alice.accountId(), key(), 1_000);

        StoredResponse r = transfers.transfer(alice.userId(), key(), alice.accountId(), bob.accountId(), 400);

        UUID txnId = txnId(r);
        List<Event> events = eventsForTxn(txnId);
        assertThat(events).hasSize(2);
        Event debit = events.get(0);
        Event credit = events.get(1);
        assertThat(debit.id()).isLessThan(credit.id());

        assertThat(debit.aggregateId()).isEqualTo(String.valueOf(alice.accountId()));
        assertThat(debit.payload().get("direction").asString()).isEqualTo("DEBIT");
        assertThat(debit.payload().get("accountId").asLong()).isEqualTo(alice.accountId());
        assertThat(debit.payload().get("ownerUserId").asLong()).isEqualTo(alice.userId());
        assertThat(debit.payload().get("ledgerEntryId").asLong())
                .isEqualTo(ledgerEntryId(txnId, alice.accountId(), "DEBIT"));

        assertThat(credit.aggregateId()).isEqualTo(String.valueOf(bob.accountId()));
        assertThat(credit.payload().get("direction").asString()).isEqualTo("CREDIT");
        assertThat(credit.payload().get("accountId").asLong()).isEqualTo(bob.accountId());
        assertThat(credit.payload().get("ownerUserId").asLong()).isEqualTo(bob.userId());
        assertThat(credit.payload().get("ledgerEntryId").asLong())
                .isEqualTo(ledgerEntryId(txnId, bob.accountId(), "CREDIT"));

        for (Event e : events) {
            assertThat(e.type()).isEqualTo(TYPE);
            assertThat(e.unpublished()).isTrue();
            assertThat(e.payload().get("transactionType").asString()).isEqualTo("TRANSFER");
            assertThat(e.payload().get("amount").asLong()).isEqualTo(400);
            assertThat(e.payload().get("transactionId").asString()).isEqualTo(txnId.toString());
        }
        assertThat(debit.payload().get("ledgerEntryId").asLong())
                .isNotEqualTo(credit.payload().get("ledgerEntryId").asLong());
        assertThat(eventsFor(alice)).hasSize(2); // funding + debit
        assertThat(eventsFor(bob)).hasSize(1);   // credit only
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    // T4
    @Test
    void insufficientFundsWritesNoEventAndNoKeyRowForWithdrawalAndTransfer() {
        Wallet alice = wallet();
        Wallet bob = wallet();
        money.deposit(alice.userId(), alice.accountId(), key(), 100);
        assertThat(eventsFor(alice)).hasSize(1);
        String withdrawKey = key();
        String transferKey = key();

        assertThatThrownBy(() -> money.withdraw(alice.userId(), alice.accountId(), withdrawKey, 101))
                .isInstanceOf(InsufficientFundsException.class);
        assertThatThrownBy(() -> transfers.transfer(alice.userId(), transferKey, alice.accountId(), bob.accountId(), 101))
                .isInstanceOf(InsufficientFundsException.class);

        assertThat(eventsFor(alice)).hasSize(1); // still only the funding event
        assertThat(eventsFor(bob)).isEmpty();
        assertThat(keyRows(alice.userId(), withdrawKey)).isZero();
        assertThat(keyRows(alice.userId(), transferKey)).isZero();
        assertThat(balance(alice)).isEqualTo(100);
        assertThat(balance(bob)).isZero();
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    // T5
    @Test
    void replayingTheSameKeyAndBodyDoesNotWriteMoreEvents() {
        Wallet alice = wallet();
        Wallet bob = wallet();

        String depositKey = key();
        money.deposit(alice.userId(), alice.accountId(), depositKey, 1_000);
        assertThat(eventsFor(alice)).hasSize(1);
        money.deposit(alice.userId(), alice.accountId(), depositKey, 1_000);
        assertThat(eventsFor(alice)).hasSize(1);

        String transferKey = key();
        StoredResponse first = transfers.transfer(alice.userId(), transferKey, alice.accountId(), bob.accountId(), 300);
        UUID txnId = txnId(first);
        assertThat(eventsForTxn(txnId)).hasSize(2);
        transfers.transfer(alice.userId(), transferKey, alice.accountId(), bob.accountId(), 300);
        assertThat(eventsForTxn(txnId)).hasSize(2);
        assertThat(eventsFor(alice)).hasSize(2);
        assertThat(eventsFor(bob)).hasSize(1);
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    // T6
    @Test
    void keyReuseWithADifferentBodyWritesNoEvent() {
        Wallet alice = wallet();
        Wallet bob = wallet();

        String depositKey = key();
        money.deposit(alice.userId(), alice.accountId(), depositKey, 500);
        assertThatThrownBy(() -> money.deposit(alice.userId(), alice.accountId(), depositKey, 600))
                .isInstanceOf(IdempotencyKeyReuseException.class);
        assertThat(eventsFor(alice)).hasSize(1);

        String transferKey = key();
        transfers.transfer(alice.userId(), transferKey, alice.accountId(), bob.accountId(), 100);
        assertThat(eventsFor(alice)).hasSize(2);
        assertThat(eventsFor(bob)).hasSize(1);
        assertThatThrownBy(() -> transfers.transfer(alice.userId(), transferKey, alice.accountId(), bob.accountId(), 101))
                .isInstanceOf(IdempotencyKeyReuseException.class);
        assertThat(eventsFor(alice)).hasSize(2);
        assertThat(eventsFor(bob)).hasSize(1);
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    // payload format pin (Jackson 3 changed some defaults)
    @Test
    void payloadNumbersAreIntegersAndOccurredAtIsIso8601Utc() {
        Wallet w = wallet();
        StoredResponse r = money.deposit(w.userId(), w.accountId(), key(), 1_234);
        UUID txnId = txnId(r);
        long eventId = eventsFor(w).get(0).id();

        for (String field : List.of("amount", "ledgerEntryId", "accountId", "ownerUserId")) {
            String type = jdbc.sql("SELECT jsonb_typeof(payload->?) FROM outbox_events WHERE id = ?")
                    .param(field).param(eventId).query(String.class).single();
            String text = jdbc.sql("SELECT payload->>? FROM outbox_events WHERE id = ?")
                    .param(field).param(eventId).query(String.class).single();
            assertThat(type).as(field + " json type").isEqualTo("number");
            assertThat(text).as(field + " text").matches("^[0-9]+$"); // never 1234.0 or 1.234E3
        }
        String occurredAt = jdbc.sql("SELECT payload->>'occurredAt' FROM outbox_events WHERE id = ?")
                .param(eventId).query(String.class).single();
        assertThat(jdbc.sql("SELECT jsonb_typeof(payload->'occurredAt') FROM outbox_events WHERE id = ?")
                .param(eventId).query(String.class).single()).isEqualTo("string");
        assertThat(occurredAt).matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?Z$");
        Instant createdAt = jdbc.sql("SELECT created_at FROM transactions WHERE id = ?")
                .param(txnId).query(OffsetDateTime.class).single().toInstant();
        assertThat(Duration.between(Instant.parse(occurredAt), createdAt).abs()).isLessThan(Duration.ofMillis(1));
    }

    private Wallet wallet() {
        User user = users.saveAndFlush(new User("outbox-" + UUID.randomUUID() + "@example.test", "x"));
        Account account = accounts.saveAndFlush(new Account(user.getId(), "SGD"));
        return new Wallet(user.getId(), account.getId());
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }

    private UUID txnId(StoredResponse r) {
        return UUID.fromString(mapper.readTree(r.body()).get("id").asString());
    }

    private List<Event> eventsFor(Wallet w) {
        return jdbc.sql("""
                        SELECT id, aggregate_id, type, published_at IS NULL AS unpublished, payload::text AS payload
                        FROM outbox_events WHERE aggregate_id = ? ORDER BY id
                        """)
                .param(String.valueOf(w.accountId()))
                .query((rs, n) -> new Event(rs.getLong("id"), rs.getString("aggregate_id"), rs.getString("type"),
                        rs.getBoolean("unpublished"), mapper.readTree(rs.getString("payload"))))
                .list();
    }

    private List<Event> eventsForTxn(UUID txnId) {
        return jdbc.sql("""
                        SELECT id, aggregate_id, type, published_at IS NULL AS unpublished, payload::text AS payload
                        FROM outbox_events WHERE payload->>'transactionId' = ? ORDER BY id
                        """)
                .param(txnId.toString())
                .query((rs, n) -> new Event(rs.getLong("id"), rs.getString("aggregate_id"), rs.getString("type"),
                        rs.getBoolean("unpublished"), mapper.readTree(rs.getString("payload"))))
                .list();
    }

    private List<String> payloadKeys(long eventId) {
        return jdbc.sql("SELECT jsonb_object_keys(payload) FROM outbox_events WHERE id = ?")
                .param(eventId).query(String.class).list();
    }

    private long ledgerEntryId(UUID txnId, long accountId, String direction) {
        return jdbc.sql("SELECT id FROM ledger_entries WHERE transaction_id = ? AND account_id = ? AND direction = ?")
                .param(txnId).param(accountId).param(direction).query(Long.class).single();
    }

    private long keyRows(long userId, String key) {
        return jdbc.sql("SELECT COUNT(*) FROM idempotency_keys WHERE user_id = ? AND key = ?")
                .param(userId).param(key).query(Long.class).single();
    }

    private long balance(Wallet w) {
        return accounts.findById(w.accountId()).orElseThrow().getBalance();
    }
}
