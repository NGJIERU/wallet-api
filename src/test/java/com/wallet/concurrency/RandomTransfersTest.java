package com.wallet.concurrency;

import com.wallet.idempotency.StoredResponse;
import com.wallet.transaction.TransferService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;

class RandomTransfersTest extends ConcurrencyTestBase {

    private static final Logger log = LoggerFactory.getLogger(RandomTransfersTest.class);

    private static final int ACCOUNTS = 8;
    private static final int THREADS = 16;
    private static final int PER_THREAD = 25;
    private static final long START_BALANCE = 10_000;

    @Autowired TransferService transfers;

    private record Plan(int from, int to, long amount) {
    }

    @Test
    void randomTransfersBetweenEightAccountsConserveMoneyAndNeverFail() {
        List<Wallet> wallets = new ArrayList<>();
        for (int i = 0; i < ACCOUNTS; i++) {
            Wallet w = wallet("SGD");
            fund(w, START_BALANCE);
            wallets.add(w);
        }

        // plans are drawn here, on one thread, from one seeded Random: the same seed gives the same plans
        Random rnd = random();
        ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
        List<Callable<List<Outcome>>> tasks = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            List<Plan> plans = new ArrayList<>();
            for (int i = 0; i < PER_THREAD; i++) {
                int from = rnd.nextInt(ACCOUNTS);
                int to = (from + 1 + rnd.nextInt(ACCOUNTS - 1)) % ACCOUNTS; // never equal to from
                plans.add(new Plan(from, to, 1 + rnd.nextInt(50)));
            }
            tasks.add(() -> execute(plans, wallets, unexpected));
        }

        ConcurrentRunner.RunResult<List<Outcome>> r = ConcurrentRunner.run(tasks);
        r.log("random-transfers (per thread)");
        r.assertNoUnexpected();

        List<Outcome> all = r.okValues().stream().flatMap(List::stream).toList();
        long ok = all.stream().filter(o -> o == Outcome.OK).count();
        long busy = all.stream().filter(o -> o == Outcome.BUSY).count();
        long insufficient = all.stream().filter(o -> o == Outcome.INSUFFICIENT).count();
        log.info("[random-transfers] transfers={} ok={} busy={} insufficient={} unexpected={} wall={}ms",
                all.size(), ok, busy, insufficient, unexpected.size(), r.wallMillis());

        assertThat(unexpected).as("UNEXPECTED outcomes, first error is the cause").isEmpty();
        assertThat(all).hasSize(THREADS * PER_THREAD);
        assertThat(busy).isZero();
        assertThat(insufficient).isZero();
        assertThat(ok).isEqualTo(THREADS * PER_THREAD);

        assertThat(totalBalance(wallets)).isEqualTo(ACCOUNTS * START_BALANCE);
        assertNoNegativeBalance(wallets);
        assertThat(ledgerRows(wallets)).isEqualTo(ACCOUNTS + 2 * ok); // 8 funding rows + 2 per transfer
        assertReconciled(jdbc, wallets.stream().mapToLong(Wallet::accountId).toArray());
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