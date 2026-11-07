package com.wallet.outbox;

import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.config.KafkaTopicConfig;
import com.wallet.idempotency.StoredResponse;
import com.wallet.transaction.MoneyService;
import com.wallet.transaction.TransferService;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** RT1 and RT2: the real relay against the real broker, scoped to the accounts each test creates. */
class OutboxRelayTest extends AbstractIntegrationTest {

    private static final String TOPIC = KafkaTopicConfig.TRANSACTION_COMPLETED_TOPIC;

    @Autowired MoneyService money;
    @Autowired TransferService transfers;
    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired OutboxRelay relay;
    @Value("${spring.kafka.bootstrap-servers}") String bootstrap;

    private record Wallet(long userId, long accountId) {
    }

    // RT1
    @Test
    void aDepositIsPublishedOnceWithKeyHeadersAndPayloadAndTheRowIsMarked() {
        Wallet w = wallet();
        String key = String.valueOf(w.accountId());
        money.deposit(w.userId(), w.accountId(), UUID.randomUUID().toString(), 1_000);
        long outboxId = jdbc.sql("SELECT id FROM outbox_events WHERE aggregate_id = ?")
                .param(key).query(Long.class).single();
        String stored = jdbc.sql("SELECT CAST(payload AS text) FROM outbox_events WHERE id = ?")
                .param(outboxId).query(String.class).single();
        assertThat(published(outboxId)).isFalse();

        RelaySupport.drain(relay);

        List<RelaySupport.Rec> recs = RelaySupport.read(bootstrap, TOPIC, Set.of(key));
        assertThat(recs).hasSize(1);
        RelaySupport.Rec r = recs.get(0);
        assertThat(r.key()).isEqualTo(key);
        assertThat(r.eventId()).isEqualTo(outboxId);
        assertThat(r.eventType()).isEqualTo("TransactionCompleted.v1");
        assertThat(mapper.readTree(r.value())).isEqualTo(mapper.readTree(stored));
        assertThat(published(outboxId)).isTrue();

        OutboxRelay.BatchResult again = relay.publishBatch();
        assertThat(again.selected()).as("nothing left to publish").isZero();
        assertThat(RelaySupport.read(bootstrap, TOPIC, Set.of(key))).as("no second record").hasSize(1);
    }

    // RT2
    @Test
    void aTransferIsPublishedAsADebitForTheSenderAndACreditForTheReceiver() {
        Wallet alice = wallet();
        Wallet bob = wallet();
        money.deposit(alice.userId(), alice.accountId(), UUID.randomUUID().toString(), 1_000);
        StoredResponse resp = transfers.transfer(alice.userId(), UUID.randomUUID().toString(),
                alice.accountId(), bob.accountId(), 400);
        String txnId = mapper.readTree(resp.body()).get("id").asString();

        RelaySupport.drain(relay);

        List<String> keys = List.of(String.valueOf(alice.accountId()), String.valueOf(bob.accountId()));
        List<RelaySupport.Rec> forTxn = RelaySupport.read(bootstrap, TOPIC, keys).stream()
                .filter(x -> txnId.equals(mapper.readTree(x.value()).get("transactionId").asString())).toList();
        assertThat(forTxn).hasSize(2);
        RelaySupport.Rec debit = forTxn.stream()
                .filter(x -> "DEBIT".equals(mapper.readTree(x.value()).get("direction").asString())).findFirst().orElseThrow();
        RelaySupport.Rec credit = forTxn.stream()
                .filter(x -> "CREDIT".equals(mapper.readTree(x.value()).get("direction").asString())).findFirst().orElseThrow();
        assertThat(debit.key()).isEqualTo(String.valueOf(alice.accountId()));
        assertThat(credit.key()).isEqualTo(String.valueOf(bob.accountId()));
        assertThat(debit.eventId()).isLessThan(credit.eventId());
        assertThat(debit.eventId()).isNotEqualTo(credit.eventId());
        long unpublished = jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id IN (?, ?) AND published_at IS NULL")
                .param(keys.get(0)).param(keys.get(1)).query(Long.class).single();
        assertThat(unpublished).isZero();
    }

    private boolean published(long outboxId) {
        return jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events WHERE id = ?")
                .param(outboxId).query(Boolean.class).single();
    }

    private Wallet wallet() {
        User user = users.saveAndFlush(new User("relay-" + UUID.randomUUID() + "@example.test", "x"));
        Account account = accounts.saveAndFlush(new Account(user.getId(), "SGD"));
        return new Wallet(user.getId(), account.getId());
    }
}
