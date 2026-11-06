package com.wallet.concurrency;

import com.wallet.account.Account;
import com.wallet.common.PgErrors;
import com.wallet.common.TransactionRetry;
import com.wallet.outbox.OutboxWriter;
import com.wallet.transaction.Direction;
import com.wallet.transaction.LedgerEntry;
import com.wallet.transaction.Transaction;
import com.wallet.transaction.TransactionType;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * T8. Same forced deadlock as RetryOnRealDeadlockTest (the real services lock in id order and cannot deadlock),
 * but each transaction also writes its events through the real OutboxWriter. The victim's rolled-back first
 * attempt must leave no event.
 */
class OutboxDeadlockRetryTest extends ConcurrencyTestBase {

    @Autowired PlatformTransactionManager txManager;
    @Autowired OutboxWriter outbox;
    @PersistenceContext EntityManager em;

    private static final class Side {
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger commits = new AtomicInteger();
        final AtomicBoolean sawDeadlock = new AtomicBoolean();
    }

    @Test
    void aDeadlockVictimLeavesNoEventFromItsFailedAttemptAndEventsMatchLedgerRows() {
        Wallet a = wallet("SGD");
        Wallet b = wallet("SGD");
        fund(a, 1_000);
        fund(b, 1_000);
        List<Long> ids = List.of(a.accountId(), b.accountId());
        TransactionTemplate tx = new TransactionTemplate(txManager);
        TransactionRetry retry = new TransactionRetry(3, 25);
        CountDownLatch bothHoldFirst = new CountDownLatch(2);
        Side s1 = new Side();
        Side s2 = new Side();

        List<Callable<Integer>> tasks = List.of(
                task(tx, retry, bothHoldFirst, s1, a, b, 10),
                task(tx, retry, bothHoldFirst, s2, b, a, 30));
        ConcurrentRunner.RunResult<Integer> r = ConcurrentRunner.run(tasks);
        r.log("outbox-retry-deadlock");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(2);
        assertThat(s1.attempts.get() + s2.attempts.get()).isEqualTo(3); // exactly one victim, retried once
        assertThat(s1.sawDeadlock.get() ^ s2.sawDeadlock.get()).isTrue(); // a real 40P01
        assertThat(s1.commits.get()).isEqualTo(1);
        assertThat(s2.commits.get()).isEqualTo(1);

        assertThat(balance(a)).isEqualTo(1_020);
        assertThat(balance(b)).isEqualTo(980);
        // 2 funding + 2 transfers x 2 entries = 6 ledger rows, so exactly 6 events
        OutboxCheck.assertOneToOne(jdbc, "T8", ids, 6);
        assertThat(OutboxCheck.eventsWhere(jdbc, ids, "transactionType", "TRANSFER")).isEqualTo(4);
        assertReconciled(jdbc, a.accountId(), b.accountId());
    }

    private Callable<Integer> task(TransactionTemplate tx, TransactionRetry retry, CountDownLatch bothHoldFirst,
                                   Side side, Wallet from, Wallet to, long amount) {
        return () -> retry.run(() -> {
            try {
                Integer attempt = tx.execute(status -> {
                    int n = side.attempts.incrementAndGet();
                    Account source = accounts.lockAllByIdOrdered(List.of(from.accountId())).get(0);
                    if (n == 1) {
                        awaitBoth(bothHoldFirst);
                    }
                    Account target = accounts.lockAllByIdOrdered(List.of(to.accountId())).get(0);

                    source.debit(amount);
                    target.credit(amount);
                    Transaction txn = new Transaction(TransactionType.TRANSFER, "SGD", amount, from.userId());
                    em.persist(txn);
                    em.flush(); // the transaction row must exist before the ledger rows (foreign key)
                    LedgerEntry debit = new LedgerEntry(txn.getId(), from.accountId(), Direction.DEBIT, amount);
                    em.persist(debit); // IDENTITY: inserted now, id populated
                    outbox.write(source, txn, debit);
                    LedgerEntry credit = new LedgerEntry(txn.getId(), to.accountId(), Direction.CREDIT, amount);
                    em.persist(credit);
                    outbox.write(target, txn, credit);
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

    private static void awaitBoth(CountDownLatch latch) {
        latch.countDown();
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the other thread never reached the barrier");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
