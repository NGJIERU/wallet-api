package com.wallet.concurrency;

import com.wallet.idempotency.StoredResponse;
import com.wallet.transaction.TransferService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;

/** T9, T10, T11: outbox events under concurrency. */
class OutboxConcurrencyTest extends ConcurrencyTestBase {

    private static final int ACCOUNTS = 8;
    private static final int THREADS = 16;
    private static final int PER_THREAD = 25;
    private static final long START_BALANCE = 10_000;

    @Autowired TransferService transfers;

    private record Plan(int from, int to, long amount) {
    }

    // T9
    @Test
    void randomConcurrentTransfersGiveExactlyOneEventPerLedgerEntryInPerAccountOrder() {
        List<Wallet> wallets = new ArrayList<>();
        for (int i = 0; i < ACCOUNTS; i++) {
            Wallet w = wallet("SGD");
            fund(w, START_BALANCE);
            wallets.add(w);
        }
        List<Long> ids = wallets.stream().map(Wallet::accountId).toList();

        Random rnd = random();
        ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
        List<Callable<List<Outcome>>> tasks = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            List<Plan> plans = new ArrayList<>();
            for (int i = 0; i < PER_THREAD; i++) {
                int from = rnd.nextInt(ACCOUNTS);
                int to = (from + 1 + rnd.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
                plans.add(new Plan(from, to, 1 + rnd.nextInt(50)));
            }
            tasks.add(() -> execute(plans, wallets, unexpected));
        }
        ConcurrentRunner.RunResult<List<Outcome>> r = ConcurrentRunner.run(tasks);
        r.log("outbox-random-transfers");
        r.assertNoUnexpected();

        List<Outcome> all = r.okValues().stream().flatMap(List::stream).toList();
        assertThat(unexpected).isEmpty();
        assertThat(all).hasSize(THREADS * PER_THREAD);
        assertThat(all).allMatch(o -> o == Outcome.OK);

        assertThat(OutboxCheck.eventsWhere(jdbc, ids, "transactionType", "TRANSFER"))
                .as("transfer events").isEqualTo(2L * THREADS * PER_THREAD); // exactly 800
        // 8 funding rows + 800 transfer rows, one event each
        OutboxCheck.assertOneToOne(jdbc, "T9", ids, ACCOUNTS + 2L * THREADS * PER_THREAD);
        assertThat(totalBalance(wallets)).isEqualTo(ACCOUNTS * START_BALANCE);
        assertReconciled(jdbc, ids.stream().mapToLong(Long::longValue).toArray());
    }

    // T10
    @Test
    void hotAccountWritesEventsOnlyForSuccessfulWithdrawals() {
        Wallet a = wallet("SGD");
        fund(a, 1_000);
        List<Long> ids = List.of(a.accountId());

        List<Callable<StoredResponse>> tasks = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            tasks.add(() -> money.withdraw(a.userId(), a.accountId(), key(), 100));
        }
        ConcurrentRunner.RunResult<StoredResponse> r = ConcurrentRunner.run(tasks);
        r.log("outbox-hot-account");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(10);
        assertThat(r.count(Outcome.INSUFFICIENT)).isEqualTo(40);
        assertThat(OutboxCheck.events(jdbc, ids)).isEqualTo(11); // funding + 10 withdrawals, none for the 40 refused
        assertThat(OutboxCheck.eventsWhere(jdbc, ids, "direction", "DEBIT")).isEqualTo(10);
        assertThat(OutboxCheck.eventsWhere(jdbc, ids, "direction", "CREDIT")).isEqualTo(1);
        OutboxCheck.assertOneToOne(jdbc, "T10", ids, 11);
        assertReconciled(jdbc, a.accountId());
    }

    // T11a
    @Test
    void twentyThreadsWithTheSameDepositKeyWriteExactlyOneEvent() {
        Wallet w = wallet("SGD");
        List<Long> ids = List.of(w.accountId());
        String k = key();

        List<Callable<StoredResponse>> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            tasks.add(() -> money.deposit(w.userId(), w.accountId(), k, 100));
        }
        ConcurrentRunner.RunResult<StoredResponse> r = ConcurrentRunner.run(tasks);
        r.log("outbox-key-race-deposit");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(20);
        assertThat(OutboxCheck.events(jdbc, ids)).isEqualTo(1);
        OutboxCheck.assertOneToOne(jdbc, "T11a", ids, 1);
        assertThat(balance(w)).isEqualTo(100);
        assertReconciled(jdbc, w.accountId());
    }

    // T11b
    @Test
    void twentyThreadsWithTheSameTransferKeyWriteExactlyTwoEvents() {
        Wallet alice = wallet("SGD");
        Wallet bob = wallet("SGD");
        fund(alice, 1_000);
        List<Long> ids = List.of(alice.accountId(), bob.accountId());
        String k = key();

        List<Callable<StoredResponse>> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            tasks.add(() -> transfers.transfer(alice.userId(), k, alice.accountId(), bob.accountId(), 300));
        }
        ConcurrentRunner.RunResult<StoredResponse> r = ConcurrentRunner.run(tasks);
        r.log("outbox-key-race-transfer");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(20);
        assertThat(OutboxCheck.eventsWhere(jdbc, ids, "transactionType", "TRANSFER")).isEqualTo(2);
        OutboxCheck.assertOneToOne(jdbc, "T11b", ids, 3); // funding + debit + credit
        assertReconciled(jdbc, alice.accountId(), bob.accountId());
    }

    private List<Outcome> execute(List<Plan> plans, List<Wallet> wallets, ConcurrentLinkedQueue<Throwable> unexpected) {
        List<Outcome> outcomes = new ArrayList<>();
        for (Plan p : plans) {
            Wallet src = wallets.get(p.from());
            Wallet dst = wallets.get(p.to());
            try {
                StoredResponse resp = transfers.transfer(src.userId(), key(), src.accountId(), dst.accountId(), p.amount());
                outcomes.add(resp.code() == 201 ? Outcome.OK : Outcome.UNEXPECTED);
            } catch (RuntimeException e) {
                Outcome o = Outcome.classify(e);
                if (o == Outcome.UNEXPECTED) {
                    unexpected.add(e);
                }
                outcomes.add(o);
            }
        }
        return outcomes;
    }
}
