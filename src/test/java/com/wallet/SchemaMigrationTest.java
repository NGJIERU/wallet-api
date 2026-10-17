package com.wallet;

import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs against a real Postgres (Testcontainers). Booting the context proves three things at once:
 * Flyway applies V1, Hibernate's ddl-auto=validate accepts the entity mapping, and the app starts.
 */
@SpringBootTest
@Testcontainers
class SchemaMigrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    UserRepository users;
    @Autowired
    AccountRepository accounts;

    @Test
    void flywayCreatesAllTables() {
        List<String> tables = jdbc.queryForList(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", String.class);

        assertThat(tables).contains(
                "users", "accounts", "transactions", "ledger_entries",
                "idempotency_keys", "outbox_events", "processed_events");
    }

    @Test
    void outboxHasPartialIndexOnUnpublishedRows() {
        String indexDef = jdbc.queryForObject(
                "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_outbox_unpublished'", String.class);

        assertThat(indexDef).contains("WHERE (published_at IS NULL)");
    }

    @Test
    void userAndAccountMappingsRoundTrip() {
        User user = users.save(new User(uniqueEmail(), "hash"));
        Account saved = accounts.save(new Account(user.getId(), "MYR"));

        Account loaded = accounts.findById(saved.getId()).orElseThrow();

        assertThat(loaded.getUserId()).isEqualTo(user.getId());
        assertThat(loaded.getCurrency()).isEqualTo("MYR");
        assertThat(loaded.getBalance()).isZero();
    }

    @Test
    void databaseRejectsNegativeBalance() {
        long accountId = createAccount("USD");

        assertThatThrownBy(() -> jdbc.update("UPDATE accounts SET balance = -1 WHERE id = ?", accountId))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void databaseRejectsSecondAccountInSameCurrencyForOneUser() {
        User user = users.save(new User(uniqueEmail(), "hash"));
        accounts.saveAndFlush(new Account(user.getId(), "SGD"));

        assertThatThrownBy(() -> accounts.saveAndFlush(new Account(user.getId(), "SGD")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void databaseRejectsMalformedCurrencyCode() {
        User user = users.save(new User(uniqueEmail(), "hash"));

        assertThatThrownBy(() -> accounts.saveAndFlush(new Account(user.getId(), "myr")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void databaseRejectsNonPositiveLedgerAmount() {
        long accountId = createAccount("EUR");
        long userId = jdbc.queryForObject("SELECT user_id FROM accounts WHERE id = ?", Long.class, accountId);
        UUID txId = UUID.randomUUID();
        jdbc.update("INSERT INTO transactions (id, type, currency, amount, created_by) VALUES (?, 'DEPOSIT', 'EUR', 100, ?)",
                txId, userId);

        assertThatThrownBy(() -> jdbc.update(
                "INSERT INTO ledger_entries (transaction_id, account_id, direction, amount) VALUES (?, ?, 'CREDIT', 0)",
                txId, accountId))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private long createAccount(String currency) {
        User user = users.save(new User(uniqueEmail(), "hash"));
        return accounts.save(new Account(user.getId(), currency)).getId();
    }

    private static String uniqueEmail() {
        return "user-" + UUID.randomUUID() + "@example.test";
    }
}
