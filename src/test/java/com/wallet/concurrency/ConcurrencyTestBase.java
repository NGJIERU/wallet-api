package com.wallet.concurrency;

import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.transaction.MoneyService;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** One shared Spring context for every heavy test: long lock timeout, 50-connection pool. */
@Tag("concurrency")
@TestPropertySource(properties = {
        "app.lock-timeout-ms=30000",
        "spring.datasource.hikari.maximum-pool-size=50",
        "spring.datasource.hikari.minimum-idle=2"})
public abstract class ConcurrencyTestBase extends AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTestBase.class);

    @Autowired protected UserRepository users;
    @Autowired protected AccountRepository accounts;
    @Autowired protected MoneyService money;
    @Autowired protected JdbcClient jdbc;

    public record Wallet(long userId, long accountId) {
    }

    protected Wallet wallet(String currency) {
        User user = users.saveAndFlush(new User("conc-" + UUID.randomUUID() + "@example.test", "x"));
        Account account = accounts.saveAndFlush(new Account(user.getId(), currency));
        return new Wallet(user.getId(), account.getId());
    }

    protected void fund(Wallet w, long amount) {
        money.deposit(w.userId(), w.accountId(), key(), amount);
    }

    protected static String key() {
        return UUID.randomUUID().toString();
    }

    /** Seeded Random; the seed is logged so a failing run can be replayed. */
    protected static Random random() {
        return random(System.nanoTime());
    }

    protected static Random random(long seed) {
        log.info("random seed = {}", seed);
        return new Random(seed);
    }

    protected long balance(Wallet w) {
        return accounts.findById(w.accountId()).orElseThrow().getBalance();
    }

    protected long totalBalance(List<Wallet> wallets) {
        List<Long> ids = wallets.stream().map(Wallet::accountId).toList();
        return jdbc.sql("SELECT COALESCE(SUM(balance), 0) FROM accounts WHERE id IN (:ids)")
                .param("ids", ids).query(Long.class).single();
    }

    protected long ledgerRows(List<Wallet> wallets) {
        List<Long> ids = wallets.stream().map(Wallet::accountId).toList();
        return jdbc.sql("SELECT COUNT(*) FROM ledger_entries WHERE account_id IN (:ids)")
                .param("ids", ids).query(Long.class).single();
    }

    protected long keyRows(long userId, String key) {
        return jdbc.sql("SELECT COUNT(*) FROM idempotency_keys WHERE user_id = ? AND key = ?")
                .param(userId).param(key).query(Long.class).single();
    }

    protected void assertNoNegativeBalance(List<Wallet> wallets) {
        List<Long> ids = wallets.stream().map(Wallet::accountId).toList();
        long negative = jdbc.sql("SELECT COUNT(*) FROM accounts WHERE id IN (:ids) AND balance < 0")
                .param("ids", ids).query(Long.class).single();
        assertThat(negative).isZero();
    }
}