package com.wallet.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountNotFoundException;
import com.wallet.account.AccountRepository;
import com.wallet.account.DestinationAccountNotFoundException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransferServiceTest extends AbstractIntegrationTest {

    @Autowired TransferService transfers;
    @Autowired MoneyService money;
    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired JdbcClient jdbc;
    @Autowired ObjectMapper mapper;

    private record Wallet(long userId, long accountId) {
    }

    @Test
    void transferMovesMoneyAndWritesADebitAndACreditUnderOneTransaction() throws Exception {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD"); // another user: transfers between users are allowed
        fund(alice, 1_000);

        StoredResponse r = transfers.transfer(alice.userId(), key(), alice.accountId(), bob.accountId(), 400);

        assertThat(r.code()).isEqualTo(201);
        JsonNode body = mapper.readTree(r.body());
        assertThat(body.get("type").asText()).isEqualTo("TRANSFER");
        assertThat(body.get("amount").asLong()).isEqualTo(400);
        assertThat(body.get("fromAccountId").asLong()).isEqualTo(alice.accountId());
        assertThat(body.get("toAccountId").asLong()).isEqualTo(bob.accountId());
        assertThat(body.get("fromBalanceAfter").asLong()).isEqualTo(600);
        assertThat(balance(alice)).isEqualTo(600);
        assertThat(balance(bob)).isEqualTo(400);

        UUID txnId = UUID.fromString(body.get("id").asText());
        assertThat(entriesFor(txnId)).isEqualTo(2);
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    @Test
    void insufficientFundsLeavesNoPartialWritesAndDoesNotConsumeTheKey() {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD");
        fund(alice, 100);
        String k = key();

        assertThatThrownBy(() -> transfers.transfer(alice.userId(), k, alice.accountId(), bob.accountId(), 101))
                .isInstanceOf(InsufficientFundsException.class);

        assertThat(balance(alice)).isEqualTo(100);
        assertThat(balance(bob)).isZero();
        assertThat(ledgerRows(alice)).isEqualTo(1); // only the funding deposit
        assertThat(ledgerRows(bob)).isZero();
        assertThat(keyRows(alice.userId(), k)).isZero();
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    @Test
    void sameKeyAndSameRequestReplaysWithoutADeductionSecondTime() throws Exception {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD");
        fund(alice, 1_000);
        String k = key();

        StoredResponse first = transfers.transfer(alice.userId(), k, alice.accountId(), bob.accountId(), 300);
        StoredResponse second = transfers.transfer(alice.userId(), k, alice.accountId(), bob.accountId(), 300);

        assertThat(second.code()).isEqualTo(201);
        assertThat(mapper.readTree(second.body()).get("id")).isEqualTo(mapper.readTree(first.body()).get("id"));
        assertThat(balance(alice)).isEqualTo(700);
        assertThat(balance(bob)).isEqualTo(300);
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    @Test
    void sameKeyWithADifferentBodyIsRejectedAndChangesNothing() {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD");
        Wallet carol = wallet("SGD");
        fund(alice, 1_000);
        String k = key();
        transfers.transfer(alice.userId(), k, alice.accountId(), bob.accountId(), 300);

        // different amount
        assertThatThrownBy(() -> transfers.transfer(alice.userId(), k, alice.accountId(), bob.accountId(), 301))
                .isInstanceOf(IdempotencyKeyReuseException.class);
        // different destination
        assertThatThrownBy(() -> transfers.transfer(alice.userId(), k, alice.accountId(), carol.accountId(), 300))
                .isInstanceOf(IdempotencyKeyReuseException.class);

        assertThat(balance(alice)).isEqualTo(700);
        assertThat(balance(bob)).isEqualTo(300);
        assertThat(balance(carol)).isZero();
        assertReconciled(jdbc, alice.accountId(), bob.accountId(), carol.accountId());
    }

    @Test
    void youCannotSendFromSomeoneElsesAccount() {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD");
        Wallet mallory = wallet("SGD");
        fund(alice, 1_000);

        assertThatThrownBy(() -> transfers.transfer(mallory.userId(), key(), alice.accountId(), mallory.accountId(), 500))
                .isInstanceOf(AccountNotFoundException.class)
                .hasMessage("Account not found");

        assertThat(balance(alice)).isEqualTo(1_000);
        assertThat(balance(mallory)).isZero();
        assertReconciled(jdbc, alice.accountId(), bob.accountId(), mallory.accountId());
    }

    @Test
    void aMissingDestinationSaysWhichAccountWasNotFoundAndChangesNothing() {
        Wallet alice = wallet("SGD");
        fund(alice, 1_000);
        String k = key();

        assertThatThrownBy(() -> transfers.transfer(alice.userId(), k, alice.accountId(), 999_999_999_999L, 100))
                .isInstanceOf(DestinationAccountNotFoundException.class)
                .hasMessage("Destination account 999999999999 not found");

        assertThat(balance(alice)).isEqualTo(1_000);
        assertThat(keyRows(alice.userId(), k)).isZero();
        assertReconciled(jdbc, alice.accountId());
    }

    @Test
    void sameAccountIsRejected() {
        Wallet alice = wallet("SGD");
        fund(alice, 1_000);

        assertThatThrownBy(() -> transfers.transfer(alice.userId(), key(), alice.accountId(), alice.accountId(), 100))
                .isInstanceOf(InvalidTransferException.class)
                .hasMessage("Cannot transfer to the same account");

        assertThat(balance(alice)).isEqualTo(1_000);
        assertReconciled(jdbc, alice.accountId());
    }

    @Test
    void currencyMismatchIsRejectedAndChangesNothing() {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("USD");
        fund(alice, 1_000);

        assertThatThrownBy(() -> transfers.transfer(alice.userId(), key(), alice.accountId(), bob.accountId(), 100))
                .isInstanceOf(InvalidTransferException.class)
                .hasMessage("Currency mismatch: SGD to USD");

        assertThat(balance(alice)).isEqualTo(1_000);
        assertThat(balance(bob)).isZero();
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    @Test
    void aMissingOrBlankKeyIsRejectedBeforeAnythingElse() {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD");

        assertThatThrownBy(() -> transfers.transfer(alice.userId(), null, alice.accountId(), bob.accountId(), 100))
                .isInstanceOf(InvalidIdempotencyKeyException.class);
        assertThatThrownBy(() -> transfers.transfer(alice.userId(), " ", alice.accountId(), bob.accountId(), 100))
                .isInstanceOf(InvalidIdempotencyKeyException.class);
    }

    // Light on purpose (2 threads x 5 transfers); the heavy version belongs to WP4. This checks that
    // opposite-direction transfers finish with no failure and nothing lost. The lock ORDER itself is
    // proven by the deadlock spike, and this test would still pass if the retry wrapper rescued a deadlock.
    @Test
    void oppositeTransfersAtTheSameTimeBothFinishAndConserveMoney() throws Exception {
        Wallet a = wallet("SGD");
        Wallet b = wallet("SGD");
        fund(a, 1_000);
        fund(b, 1_000);

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> ab = pool.submit(() -> shuttle(start, a, b));
            Future<?> ba = pool.submit(() -> shuttle(start, b, a));
            start.countDown();
            ab.get(30, TimeUnit.SECONDS); // rethrows if any transfer failed
            ba.get(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
        }

        assertThat(balance(a)).isEqualTo(1_000); // 5 x 10 out, 5 x 10 in
        assertThat(balance(b)).isEqualTo(1_000);
        assertReconciled(jdbc, a.accountId(), b.accountId());
    }

    private void shuttle(CountDownLatch start, Wallet from, Wallet to) {
        try {
            start.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        for (int i = 0; i < 5; i++) {
            transfers.transfer(from.userId(), key(), from.accountId(), to.accountId(), 10);
        }
    }

    private Wallet wallet(String currency) {
        User user = users.saveAndFlush(new User("transfer-" + UUID.randomUUID() + "@example.test", "x"));
        Account account = accounts.saveAndFlush(new Account(user.getId(), currency));
        return new Wallet(user.getId(), account.getId());
    }

    private void fund(Wallet wallet, long amount) {
        money.deposit(wallet.userId(), wallet.accountId(), key(), amount);
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }

    private long balance(Wallet wallet) {
        return accounts.findById(wallet.accountId()).orElseThrow().getBalance();
    }

    private long ledgerRows(Wallet wallet) {
        return jdbc.sql("SELECT COUNT(*) FROM ledger_entries WHERE account_id = ?")
                .param(wallet.accountId()).query(Long.class).single();
    }

    private long entriesFor(UUID transactionId) {
        return jdbc.sql("SELECT COUNT(*) FROM ledger_entries WHERE transaction_id = ?")
                .param(transactionId).query(Long.class).single();
    }

    private long keyRows(long userId, String key) {
        return jdbc.sql("SELECT COUNT(*) FROM idempotency_keys WHERE user_id = ? AND key = ?")
                .param(userId).param(key).query(Long.class).single();
    }
}