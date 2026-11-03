package com.wallet.concurrency;

import com.wallet.account.Account;
import com.wallet.common.PgErrors;
import com.wallet.common.TransactionRetry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;

class RetryOnRealDeadlockTest extends ConcurrencyTestBase {

    @Autowired PlatformTransactionManager txManager;

    /** Per-thread bookkeeping, written only by that thread's own work. */
    private static final class Side {
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger commits = new AtomicInteger();
        final AtomicBoolean sawDeadlock = new AtomicBoolean();
    }

    @Test
    void aRealDeadlockVictimIsRetriedOnceAndItsMoneyIsAppliedExactlyOnce() {
        Wallet a = wallet("SGD");
        Wallet b = wallet("SGD"); // a second user: one account per currency per user
        fund(a, 1_000);
        fund(b, 1_000);
        TransactionTemplate tx = new TransactionTemplate(txManager);
        TransactionRetry retry = new TransactionRetry(3, 25); // TransferService's own instance cannot be injected
        CountDownLatch bothHoldFirst = new CountDownLatch(2);
        Side s1 = new Side();
        Side s2 = new Side();

        // different amounts, so a double-applied retry would show in the balances
        List<Callable<Integer>> tasks = List.of(
                task(tx, retry, bothHoldFirst, s1, a, b, 10),   // A -> B 10: locks A then B
                task(tx, retry, bothHoldFirst, s2, b, a, 30));  // B -> A 30: locks B then A
        ConcurrentRunner.RunResult<Integer> r = ConcurrentRunner.run(tasks);
        r.log("retry-deadlock");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(2);
        assertThat(r.count(Outcome.BUSY)).isZero();

        // exactly one thread was the victim: one attempt for the survivor, two for the victim
        assertThat(s1.attempts.get() + s2.attempts.get()).isEqualTo(3);
        assertThat(s1.attempts.get()).isBetween(1, 2);
        assertThat(s2.attempts.get()).isBetween(1, 2);
        // the failed attempt really was a Postgres deadlock (40P01), seen by exactly one thread
        assertThat(s1.sawDeadlock.get() ^ s2.sawDeadlock.get()).isTrue();
        // both transactions finally committed, once each
        assertThat(s1.commits.get()).isEqualTo(1);
        assertThat(s2.commits.get()).isEqualTo(1);
        assertThat(r.results().get(0).value()).isEqualTo(s1.attempts.get());
        assertThat(r.results().get(1).value()).isEqualTo(s2.attempts.get());

        // both effects applied exactly once: A = 1000 - 10 + 30, B = 1000 + 10 - 30
        assertThat(balance(a)).isEqualTo(1_020);
        assertThat(balance(b)).isEqualTo(980);
        assertThat(totalBalance(List.of(a, b))).isEqualTo(2_000);
        assertThat(ledgerRows(List.of(a, b))).isEqualTo(2 + 2 + 2); // 2 funding + 2 per transfer
        assertThat(transferTransactionsTouching(a, b)).isEqualTo(2); // the rolled-back attempt left no row
        assertReconciled(jdbc, a.accountId(), b.accountId());
    }

    private Callable<Integer> task(TransactionTemplate tx, TransactionRetry retry, CountDownLatch bothHoldFirst,
                                   Side side, Wallet from, Wallet to, long amount) {
        // the supplier opens a fresh transaction on every call, as TransactionRetry requires
        return () -> retry.run(() -> {
            try {
                Integer attempt = tx.execute(status -> {
                    int n = side.attempts.incrementAndGet();
                    Account source = accounts.lockAllByIdOrdered(List.of(from.accountId())).get(0);
                    if (n == 1) { // only the first attempt is forced to deadlock; the retry must run free
                        awaitBoth(bothHoldFirst);
                    }
                    Account target = accounts.lockAllByIdOrdered(List.of(to.accountId())).get(0);

                    source.debit(amount);
                    target.credit(amount);
                    UUID txnId = UUID.randomUUID();
                    jdbc.sql("INSERT INTO transactions (id, type, currency, amount, created_by) VALUES (?, 'TRANSFER', 'SGD', ?, ?)")
                            .param(txnId).param(amount).param(from.userId()).update();
                    insertEntry(txnId, from.accountId(), "DEBIT", amount);
                    insertEntry(txnId, to.accountId(), "CREDIT", amount);
                    return n;
                });
                side.commits.incrementAndGet();
                return attempt;
            } catch (RuntimeException e) {
                if (PgErrors.isDeadlock(e)) {
                    side.sawDeadlock.set(true);
                }
                throw e;
            }
        });
    }

    private void insertEntry(UUID txnId, long accountId, String direction, long amount) {
        jdbc.sql("INSERT INTO ledger_entries (transaction_id, account_id, direction, amount) VALUES (?, ?, ?, ?)")
                .param(txnId).param(accountId).param(direction).param(amount).update();
    }

    private long transferTransactionsTouching(Wallet a, Wallet b) {
        return jdbc.sql("""
                        SELECT COUNT(*) FROM transactions t
                        WHERE t.type = 'TRANSFER'
                          AND EXISTS (SELECT 1 FROM ledger_entries l
                                      WHERE l.transaction_id = t.id AND l.account_id IN (:ids))
                        """)
                .param("ids", List.of(a.accountId(), b.accountId())).query(Long.class).single();
    }

    private static void awaitBoth(CountDownLatch bothHoldFirst) {
        bothHoldFirst.countDown();
        try {
            if (!bothHoldFirst.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the other thread never reached the barrier");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}