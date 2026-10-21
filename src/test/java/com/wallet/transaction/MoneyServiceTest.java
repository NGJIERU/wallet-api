package com.wallet.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountNotFoundException;
import com.wallet.account.AccountRepository;
import com.wallet.account.InsufficientFundsException;
import com.wallet.idempotency.IdempotencyKeyReuseException;
import com.wallet.idempotency.InvalidIdempotencyKeyException;
import com.wallet.idempotency.StoredResponse;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MoneyServiceTest extends AbstractIntegrationTest {

    @Autowired MoneyService money;
    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;

    private record Fixture(long userId, long accountId) {
    }

    @Test
    void depositCreditsTheAccountAndWritesOneLedgerEntry() throws Exception {
        Fixture f = fixture();

        StoredResponse r = money.deposit(f.userId(), f.accountId(), key(), 1_000);

        assertThat(r.code()).isEqualTo(201);
        JsonNode body = mapper.readTree(r.body());
        assertThat(body.get("type").asText()).isEqualTo("DEPOSIT");
        assertThat(body.get("amount").asLong()).isEqualTo(1_000);
        assertThat(body.get("balanceAfter").asLong()).isEqualTo(1_000);
        assertThat(balance(f.accountId())).isEqualTo(1_000);
        assertThat(ledgerNet(f.accountId())).isEqualTo(1_000);
        assertThat(ledgerRows(f.accountId())).isEqualTo(1);
    }

    @Test
    void withdrawalDebitsTheAccountAndTheLedgerStillMatchesTheBalance() {
        Fixture f = fixture();
        money.deposit(f.userId(), f.accountId(), key(), 1_000);

        money.withdraw(f.userId(), f.accountId(), key(), 400);

        assertThat(balance(f.accountId())).isEqualTo(600);
        assertThat(ledgerNet(f.accountId())).isEqualTo(600); // SUM(CREDIT) - SUM(DEBIT) == balance
    }

    @Test
    void withdrawalBeyondTheBalanceFailsAndWritesNothing() {
        Fixture f = fixture();
        money.deposit(f.userId(), f.accountId(), key(), 100);

        assertThatThrownBy(() -> money.withdraw(f.userId(), f.accountId(), key(), 101))
                .isInstanceOf(InsufficientFundsException.class);

        assertThat(balance(f.accountId())).isEqualTo(100);
        assertThat(ledgerRows(f.accountId())).isEqualTo(1); // only the deposit
    }

    @Test
    void sameKeyAndSameRequestReplaysWithoutMovingMoneyTwice() throws Exception {
        Fixture f = fixture();
        String k = key();

        StoredResponse first = money.deposit(f.userId(), f.accountId(), k, 500);
        StoredResponse second = money.deposit(f.userId(), f.accountId(), k, 500);

        assertThat(second.code()).isEqualTo(201);
        assertThat(mapper.readTree(second.body()).get("id")).isEqualTo(mapper.readTree(first.body()).get("id"));
        assertThat(balance(f.accountId())).isEqualTo(500);
        assertThat(ledgerRows(f.accountId())).isEqualTo(1);
    }

    @Test
    void sameKeyWithADifferentAmountIsRejectedAndChangesNothing() {
        Fixture f = fixture();
        String k = key();
        money.deposit(f.userId(), f.accountId(), k, 500);

        assertThatThrownBy(() -> money.deposit(f.userId(), f.accountId(), k, 600))
                .isInstanceOf(IdempotencyKeyReuseException.class);

        assertThat(balance(f.accountId())).isEqualTo(500);
    }

    @Test
    void aFailedRequestDoesNotConsumeItsKey() {
        Fixture f = fixture();
        String k = key();

        // fails: empty account. The rollback also rolls back the key claim.
        assertThatThrownBy(() -> money.withdraw(f.userId(), f.accountId(), k, 100))
                .isInstanceOf(InsufficientFundsException.class);

        money.deposit(f.userId(), f.accountId(), key(), 100);

        // the same key and request now run for real (not a replay of the failure)
        StoredResponse retry = money.withdraw(f.userId(), f.accountId(), k, 100);
        assertThat(retry.code()).isEqualTo(201);
        assertThat(balance(f.accountId())).isZero();
    }

    @Test
    void anotherUsersAccountLooksLikeAMissingOne() {
        Fixture owner = fixture();
        Fixture stranger = fixture();

        assertThatThrownBy(() -> money.deposit(stranger.userId(), owner.accountId(), key(), 100))
                .isInstanceOf(AccountNotFoundException.class);

        assertThat(balance(owner.accountId())).isZero();
    }

    @Test
    void aMissingOrBlankKeyIsRejectedBeforeAnythingElse() {
        Fixture f = fixture();

        assertThatThrownBy(() -> money.deposit(f.userId(), f.accountId(), null, 100))
                .isInstanceOf(InvalidIdempotencyKeyException.class);
        assertThatThrownBy(() -> money.deposit(f.userId(), f.accountId(), " ", 100))
                .isInstanceOf(InvalidIdempotencyKeyException.class);
    }

    private Fixture fixture() {
        User user = users.saveAndFlush(new User("money-" + UUID.randomUUID() + "@example.test", "x"));
        Account account = accounts.saveAndFlush(new Account(user.getId(), "SGD"));
        return new Fixture(user.getId(), account.getId());
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }

    private long balance(long accountId) {
        return accounts.findById(accountId).orElseThrow().getBalance();
    }

    private long ledgerNet(long accountId) {
        return jdbc.sql("""
                        SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END), 0)
                        FROM ledger_entries WHERE account_id = ?
                        """)
                .param(accountId).query(Long.class).single();
    }

    private long ledgerRows(long accountId) {
        return jdbc.sql("SELECT COUNT(*) FROM ledger_entries WHERE account_id = ?")
                .param(accountId).query(Long.class).single();
    }
}