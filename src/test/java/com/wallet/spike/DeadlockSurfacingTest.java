package com.wallet.spike;

import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.common.PgErrors;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

// Spike (WP 3.4): what does a Postgres deadlock (SQLSTATE 40P01) look like when it reaches Java?
class DeadlockSurfacingTest extends AbstractIntegrationTest {

    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired PlatformTransactionManager txManager;

    @Test
    void oppositeLockOrderDeadlocksAndPostgresKillsExactlyOneVictim() throws Exception {
        User user = users.saveAndFlush(new User("deadlock-" + UUID.randomUUID() + "@example.test", "x"));
        long a = accounts.saveAndFlush(new Account(user.getId(), "SGD")).getId();
        long b = accounts.saveAndFlush(new Account(user.getId(), "USD")).getId();

        TransactionTemplate tx = new TransactionTemplate(txManager);
        CountDownLatch bothHoldFirst = new CountDownLatch(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        Future<?> t1 = pool.submit(() -> lockThenLock(tx, bothHoldFirst, a, b));
        Future<?> t2 = pool.submit(() -> lockThenLock(tx, bothHoldFirst, b, a));

        List<Throwable> failures = new ArrayList<>();
        for (Future<?> f : List.of(t1, t2)) {
            try {
                f.get(30, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                failures.add(e.getCause());
            }
        }
        pool.shutdown();

        assertThat(failures).hasSize(1); // Postgres aborts one transaction; the other completes

        Throwable victim = failures.get(0);
        for (Throwable t = victim; t != null && t.getCause() != t; t = t.getCause()) {
            System.out.println("DEADLOCK chain: " + t.getClass().getName() + " | " + t.getMessage());
        }
        System.out.println("DEADLOCK is PessimisticLockingFailureException: "
                + (victim instanceof PessimisticLockingFailureException));

        assertThat(PgErrors.isDeadlock(victim)).isTrue();
        assertThat(PgErrors.isRetryable(victim)).isTrue();
        assertThat(PgErrors.isLockTimeout(victim)).isFalse();
    }

    // Takes `first`, waits until the other thread also holds its first row, then asks for `second`.
    private void lockThenLock(TransactionTemplate tx, CountDownLatch bothHoldFirst, long first, long second) {
        tx.executeWithoutResult(status -> {
            accounts.lockAllByIdOrdered(List.of(first));
            bothHoldFirst.countDown();
            try {
                bothHoldFirst.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            accounts.lockAllByIdOrdered(List.of(second));
        });
    }
}