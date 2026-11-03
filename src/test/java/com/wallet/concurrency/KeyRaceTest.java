package com.wallet.concurrency;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.idempotency.StoredResponse;
import com.wallet.transaction.TransferService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;

class KeyRaceTest extends ConcurrencyTestBase {

    private static final int THREADS = 20;

    @Autowired TransferService transfers;
    @Autowired ObjectMapper mapper;

    // A: same key, same body -> one transaction, every caller gets the same 201
    @Test
    void sameKeySameBodyAppliesOnceAndEveryoneGetsTheSameResponse() throws Exception {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD");
        fund(alice, 1_000);
        String k = key();

        List<Callable<StoredResponse>> tasks = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            tasks.add(() -> transfers.transfer(alice.userId(), k, alice.accountId(), bob.accountId(), 300));
        }
        ConcurrentRunner.RunResult<StoredResponse> r = ConcurrentRunner.run(tasks);
        r.log("key-A");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(THREADS); // replays count as OK, so the count alone proves nothing
        List<StoredResponse> ok = r.okValues();
        JsonNode first = mapper.readTree(ok.get(0).body());
        for (StoredResponse s : ok) {
            assertThat(s.code()).isEqualTo(201);
            assertThat(mapper.readTree(s.body())).isEqualTo(first);
        }

        Set<UUID> ids = distinctTransactionIds(ok);
        assertThat(ids).hasSize(1);
        assertThat(transactionRows(ids)).isEqualTo(1);
        assertThat(entriesFor(ids.iterator().next())).isEqualTo(2);
        assertThat(keyRows(alice.userId(), k)).isEqualTo(1);
        assertThat(balance(alice)).isEqualTo(700); // debited once
        assertThat(balance(bob)).isEqualTo(300);
        assertThat(ledgerRows(List.of(alice, bob))).isEqualTo(1 + 2); // funding + the one transfer
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    // B: same key, a different amount on every thread -> exactly one winner, the rest are key reuse
    @Test
    void sameKeyDifferentAmountsHasOneWinnerAndTheRestAreKeyReuse() throws Exception {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD");
        fund(alice, 10_000);
        String k = key();

        List<Callable<StoredResponse>> tasks = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            long amount = 100 + i;
            tasks.add(() -> transfers.transfer(alice.userId(), k, alice.accountId(), bob.accountId(), amount));
        }
        ConcurrentRunner.RunResult<StoredResponse> r = ConcurrentRunner.run(tasks);
        r.log("key-B");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(1);
        assertThat(r.count(Outcome.KEY_REUSE)).isEqualTo(THREADS - 1);

        StoredResponse winner = r.okValues().get(0);
        long winnerAmount = mapper.readTree(winner.body()).get("amount").asLong();
        assertThat(transactionRows(distinctTransactionIds(r.okValues()))).isEqualTo(1);
        assertThat(keyRows(alice.userId(), k)).isEqualTo(1);
        assertThat(balance(alice)).isEqualTo(10_000 - winnerAmount); // only the winner's amount
        assertThat(balance(bob)).isEqualTo(winnerAmount);
        assertThat(ledgerRows(List.of(alice, bob))).isEqualTo(1 + 2);
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    // C: same key, but the first attempt fails -> the key is never consumed and nothing changes
    @Test
    void sameKeyThatFailsForInsufficientFundsConsumesNothing() {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD");
        fund(alice, 100);
        String k = key();

        List<Callable<StoredResponse>> tasks = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            tasks.add(() -> transfers.transfer(alice.userId(), k, alice.accountId(), bob.accountId(), 200));
        }
        ConcurrentRunner.RunResult<StoredResponse> r = ConcurrentRunner.run(tasks);
        r.log("key-C");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.INSUFFICIENT)).isEqualTo(THREADS);
        assertThat(keyRows(alice.userId(), k)).isZero();
        assertThat(balance(alice)).isEqualTo(100);
        assertThat(balance(bob)).isZero();
        assertThat(ledgerRows(List.of(alice, bob))).isEqualTo(1); // only the funding row
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    private Set<UUID> distinctTransactionIds(List<StoredResponse> responses) throws Exception {
        Set<UUID> ids = new LinkedHashSet<>();
        for (StoredResponse s : responses) {
            ids.add(UUID.fromString(mapper.readTree(s.body()).get("id").asText()));
        }
        return ids;
    }

    private long transactionRows(Set<UUID> ids) {
        return jdbc.sql("SELECT COUNT(*) FROM transactions WHERE id IN (:ids)")
                .param("ids", new ArrayList<>(ids)).query(Long.class).single();
    }

    private long entriesFor(UUID transactionId) {
        return jdbc.sql("SELECT COUNT(*) FROM ledger_entries WHERE transaction_id = ?")
                .param(transactionId).query(Long.class).single();
    }
}