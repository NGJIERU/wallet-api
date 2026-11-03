package com.wallet.concurrency;

import com.wallet.idempotency.StoredResponse;
import com.wallet.transaction.TransferService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;

class HotAccountTest extends ConcurrencyTestBase {

    @Autowired TransferService transfers;

    // A: 1,000 balance, 50 withdrawals of 100 -> exactly 10 can win
    @Test
    void fiftyWithdrawalsOnOneAccountSucceedExactlyTenTimes() {
        Wallet a = wallet("SGD");
        fund(a, 1_000);

        List<Callable<StoredResponse>> tasks = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            tasks.add(() -> money.withdraw(a.userId(), a.accountId(), key(), 100));
        }
        ConcurrentRunner.RunResult<StoredResponse> r = ConcurrentRunner.run(tasks);
        r.log("hot-A");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(10);
        assertThat(r.count(Outcome.INSUFFICIENT)).isEqualTo(40);
        assertThat(r.count(Outcome.BUSY)).isZero();
        assertThat(balance(a)).isZero();
        assertThat(ledgerRows(List.of(a))).isEqualTo(1 + 10); // funding + 10 withdrawals
        assertReconciled(jdbc, a.accountId());
    }

    // B: 25 deposits and 25 withdrawals racing on one account that starts empty, in shuffled order
    @Test
    void mixedDepositsAndWithdrawalsOnOneAccountStayExact() {
        Wallet w = wallet("SGD"); // starts at 0 on purpose, so early withdrawals can really fail

        List<Boolean> isDeposit = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            isDeposit.add(true);
            isDeposit.add(false);
        }
        Collections.shuffle(isDeposit, random()); // seed is logged

        List<Callable<StoredResponse>> tasks = new ArrayList<>();
        for (boolean dep : isDeposit) {
            Callable<StoredResponse> t = dep
                    ? () -> money.deposit(w.userId(), w.accountId(), key(), 100)
                    : () -> money.withdraw(w.userId(), w.accountId(), key(), 100);
            tasks.add(t);
        }
        ConcurrentRunner.RunResult<StoredResponse> r = ConcurrentRunner.run(tasks);
        r.log("hot-B");

        r.assertNoUnexpected();
        long okDeposits = 0;
        long okWithdrawals = 0;
        for (int i = 0; i < isDeposit.size(); i++) {
            Outcome o = r.results().get(i).outcome();
            if (isDeposit.get(i)) {
                assertThat(o).as("deposit %d", i).isEqualTo(Outcome.OK);
                okDeposits++;
            } else {
                assertThat(o).as("withdrawal %d", i).isIn(Outcome.OK, Outcome.INSUFFICIENT);
                if (o == Outcome.OK) okWithdrawals++;
            }
        }
        assertThat(balance(w)).isEqualTo(100 * okDeposits - 100 * okWithdrawals);
        assertThat(balance(w)).isGreaterThanOrEqualTo(0);
        assertThat(ledgerRows(List.of(w))).isEqualTo(okDeposits + okWithdrawals);
        assertReconciled(jdbc, w.accountId());
    }

    // C: one source, 20 different destinations
    @Test
    void twentyTransfersFromOneSourceToTwentyDestinationsConserveMoney() {
        Wallet src = wallet("SGD");
        fund(src, 10_000);
        List<Wallet> all = new ArrayList<>();
        all.add(src);
        List<Callable<StoredResponse>> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Wallet dst = wallet("SGD");
            all.add(dst);
            tasks.add(() -> transfers.transfer(src.userId(), key(), src.accountId(), dst.accountId(), 100));
        }
        ConcurrentRunner.RunResult<StoredResponse> r = ConcurrentRunner.run(tasks);
        r.log("hot-C");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(20);
        assertThat(r.count(Outcome.BUSY)).isZero();
        assertThat(balance(src)).isEqualTo(8_000);
        assertThat(totalBalance(all)).isEqualTo(10_000);
        assertNoNegativeBalance(all);
        assertThat(ledgerRows(all)).isEqualTo(1 + 2 * 20);
        assertReconciled(jdbc, all.stream().mapToLong(Wallet::accountId).toArray());
    }
}