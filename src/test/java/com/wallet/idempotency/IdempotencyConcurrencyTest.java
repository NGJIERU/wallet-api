package com.wallet.idempotency;

import com.wallet.AbstractIntegrationTest;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Two-thread tests of the claim. The heavy N-thread money tests belong to WP 4. */
class IdempotencyConcurrencyTest extends AbstractIntegrationTest {

    @Autowired IdempotencyService service;
    @Autowired UserRepository users;
    @Autowired PlatformTransactionManager txManager;

    private final ExecutorService pool = Executors.newFixedThreadPool(2);

    @AfterEach
    void shutDown() {
        pool.shutdownNow();
    }

    @Test
    void duplicateThatCannotGetThroughTimesOutAsConflictAndTheFirstRequestStillCommits() throws Exception {
        long userId = newUser();
        String hash = IdempotencyService.hash("POST", "/transfers", "amount=100");
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Future<?> holder = startHolder(userId, "k-timeout", hash, claimed, release, true);
        await(claimed);

        long start = System.nanoTime();
        Future<Optional<StoredResponse>> waiter = startWaiter(userId, "k-timeout", hash);
        assertThatThrownBy(() -> waiter.get(15, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IdempotencyConflictException.class);
        long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        // it blocked until lock_timeout (default 2000 ms) instead of failing instantly
        assertThat(waitedMs).isGreaterThanOrEqualTo(1_500);

        release.countDown();
        holder.get(10, TimeUnit.SECONDS);

        // the first request was unaffected: a later retry replays its stored response
        Optional<StoredResponse> replay = startWaiter(userId, "k-timeout", hash).get(10, TimeUnit.SECONDS);
        assertThat(replay).isPresent();
        assertThat(replay.get().code()).isEqualTo(201);
    }

    @Test
    void duplicateThatWaitsReplaysOnceTheFirstRequestCommits() throws Exception {
        long userId = newUser();
        String hash = IdempotencyService.hash("POST", "/transfers", "amount=100");
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Future<?> holder = startHolder(userId, "k-replay", hash, claimed, release, true);
        await(claimed);

        Future<Optional<StoredResponse>> waiter = startWaiter(userId, "k-replay", hash);
        Thread.sleep(300); // let the waiter block on the uncommitted row (the result is the same if it hasn't yet)
        release.countDown();

        Optional<StoredResponse> result = waiter.get(10, TimeUnit.SECONDS);
        holder.get(10, TimeUnit.SECONDS);
        assertThat(result).isPresent();
        assertThat(result.get().code()).isEqualTo(201);
    }

    @Test
    void duplicateThatWaitsOwnsTheKeyIfTheFirstRequestRollsBack() throws Exception {
        long userId = newUser();
        String hash = IdempotencyService.hash("POST", "/transfers", "amount=100");
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Future<?> holder = startHolder(userId, "k-rollback", hash, claimed, release, false);
        await(claimed);

        Future<Optional<StoredResponse>> waiter = startWaiter(userId, "k-rollback", hash);
        Thread.sleep(300);
        release.countDown();

        // empty means "you own this request": the rolled-back claim left no stuck key behind
        assertThat(waiter.get(10, TimeUnit.SECONDS)).isEmpty();
        holder.get(10, TimeUnit.SECONDS);
    }

    /** Claims the key, signals, waits for release, then commits (with a stored response) or rolls back. */
    private Future<?> startHolder(long userId, String key, String hash,
                                  CountDownLatch claimed, CountDownLatch release, boolean commit) {
        TransactionTemplate tx = new TransactionTemplate(txManager);
        return pool.submit(() -> tx.executeWithoutResult(status -> {
            service.claim(userId, key, hash);
            claimed.countDown();
            await(release);
            if (commit) {
                service.complete(userId, key, 201, "{}");
            } else {
                status.setRollbackOnly();
            }
        }));
    }

    /** A second request with the same key: replays if one exists, otherwise claims and completes it. */
    private Future<Optional<StoredResponse>> startWaiter(long userId, String key, String hash) {
        TransactionTemplate tx = new TransactionTemplate(txManager);
        Callable<Optional<StoredResponse>> task = () -> tx.execute(status -> {
            Optional<StoredResponse> result = service.claim(userId, key, hash);
            if (result.isEmpty()) {
                service.complete(userId, key, 201, "{}");
            }
            return result;
        });
        return pool.submit(task);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private long newUser() {
        return users.saveAndFlush(new User("idem-conc-" + UUID.randomUUID() + "@example.test", "x")).getId();
    }
}