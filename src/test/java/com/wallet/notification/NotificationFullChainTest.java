package com.wallet.notification;

import com.wallet.AbstractIntegrationTest;
import com.wallet.idempotency.StoredResponse;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.transaction.MoneyService;
import com.wallet.transaction.TransferService;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CT9: the one extra Spring context (relay AND consumer on). API-level services -> outbox -> relay -> Kafka ->
 * listener -> notifications. A unique consumer group per run; assertions are scoped to this test's own accounts
 * because the topic is shared by the whole suite (the new group first reads the old backlog; dedupe makes that safe).
 */
@TestPropertySource(properties = {
        "app.outbox.relay.enabled=true",
        "app.outbox.relay.poll-interval=100ms",
        "app.notifications.consumer.enabled=true",
        "app.notifications.consumer-group=chain-${random.uuid}",
        "app.notifications.retry.initial-backoff=50ms",
        "app.lock-timeout-ms=30000"})
class NotificationFullChainTest extends AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(NotificationFullChainTest.class);

    @Autowired MoneyService money;
    @Autowired TransferService transfers;
    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired JdbcClient jdbc;

    private record Wallet(long userId, long accountId) {
    }

    @Test
    void depositsTransfersAndARandomWorkloadEndUpAsExactlyOneNotificationPerLedgerEntryInOrder() throws Exception {
        Wallet a = wallet();
        Wallet b = wallet();
        long t0 = System.nanoTime();
        money.deposit(a.userId(), a.accountId(), key(), 10_000);
        transfers.transfer(a.userId(), key(), a.accountId(), b.accountId(), 2_500);
        awaitNotifications(List.of(a, b), 3, 90);
        assertThat(count("SELECT COUNT(*) FROM notifications WHERE user_id = ?", a.userId())).isEqualTo(2); // credit + debit
        assertThat(count("SELECT COUNT(*) FROM notifications WHERE user_id = ?", b.userId())).isEqualTo(1);
        assertThat(jdbc.sql("SELECT message FROM notifications WHERE user_id = ? AND direction = 'CREDIT'")
                .param(b.userId()).query(String.class).single()).isEqualTo("Received SGD 25.00 (transfer)");
        log.info("[CHAIN CT9] first notifications after {} ms (includes reading the old topic backlog)", (System.nanoTime() - t0) / 1_000_000);

        List<Wallet> ws = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            Wallet w = wallet();
            money.deposit(w.userId(), w.accountId(), key(), 10_000);
            ws.add(w);
        }
        Random rnd = new Random(42);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                long seed = rnd.nextLong();
                fs.add(pool.submit(() -> {
                    Random r = new Random(seed);
                    for (int i = 0; i < 25; i++) {
                        int from = r.nextInt(8);
                        int to = (from + 1 + r.nextInt(7)) % 8;
                        StoredResponse resp = transfers.transfer(ws.get(from).userId(), key(), ws.get(from).accountId(),
                                ws.get(to).accountId(), 1 + r.nextInt(50));
                        if (resp.code() != 201) throw new IllegalStateException("code " + resp.code());
                    }
                }));
            }
            for (Future<?> f : fs) f.get(60, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        long expected = 8 + 2L * 4 * 25;
        awaitNotifications(ws, expected, 90);

        for (Wallet w : ws) {
            List<Long> fromLedger = jdbc.sql("SELECT id FROM ledger_entries WHERE account_id = ? ORDER BY id")
                    .param(w.accountId()).query(Long.class).list();
            List<Long> fromNotifications = jdbc.sql("SELECT ledger_entry_id FROM notifications WHERE account_id = ? ORDER BY id")
                    .param(w.accountId()).query(Long.class).list();
            assertThat(fromNotifications).as("ledger entry ids in notification order, account " + w.accountId()).isEqualTo(fromLedger);
        }
        assertThat(jdbc.sql("SELECT COUNT(*) FROM (SELECT ledger_entry_id FROM notifications WHERE account_id IN (:ids) "
                        + "GROUP BY ledger_entry_id HAVING COUNT(*) > 1) d")
                .param("ids", ws.stream().map(Wallet::accountId).toList()).query(Long.class).single()).as("duplicates").isZero();
        log.info("[CHAIN CT9] workload notifications={} expected={} perAccountOrderOk=true", expected, expected);
    }

    private void awaitNotifications(List<Wallet> ws, long expected, int seconds) {
        List<Long> ids = ws.stream().map(Wallet::accountId).toList();
        await(() -> jdbc.sql("SELECT COUNT(*) FROM notifications WHERE account_id IN (:ids)")
                .param("ids", ids).query(Long.class).single() >= expected, seconds,
                "notifications did not reach " + expected);
    }

    private static void await(BooleanSupplier cond, int seconds, String message) {
        long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError(message + " within " + seconds + " s");
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
    }

    private long count(String sql, Object p) {
        return jdbc.sql(sql).param(p).query(Long.class).single();
    }

    private Wallet wallet() {
        User u = users.saveAndFlush(new User("chain-" + UUID.randomUUID() + "@example.test", "x"));
        Account acc = accounts.saveAndFlush(new Account(u.getId(), "SGD"));
        return new Wallet(u.getId(), acc.getId());
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
