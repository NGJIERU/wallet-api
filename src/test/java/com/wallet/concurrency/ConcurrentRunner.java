package com.wallet.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class ConcurrentRunner {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentRunner.class);
    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);

    private ConcurrentRunner() {
    }

    record Result<T>(Outcome outcome, T value, Throwable error) {
    }

    record RunResult<T>(List<Result<T>> results, long wallMillis) {

        long count(Outcome o) {
            return results.stream().filter(r -> r.outcome() == o).count();
        }

        Map<Outcome, Long> counts() {
            Map<Outcome, Long> m = new EnumMap<>(Outcome.class);
            for (Outcome o : Outcome.values()) m.put(o, count(o));
            return m;
        }

        List<T> okValues() {
            return results.stream().filter(r -> r.outcome() == Outcome.OK).map(Result::value).toList();
        }

        void assertNoUnexpected() {
            List<Throwable> bad = results.stream()
                    .filter(r -> r.outcome() == Outcome.UNEXPECTED).map(Result::error).toList();
            if (!bad.isEmpty()) {
                throw new AssertionError(bad.size() + " UNEXPECTED outcome(s); first: " + bad.get(0), bad.get(0));
            }
        }

        void log(String label) {
            log.info("[{}] threads={} wall={}ms outcomes={}", label, results.size(), wallMillis, counts());
        }
    }

    static <T> RunResult<T> run(List<? extends Callable<T>> tasks) {
        return run(tasks, DEFAULT_TIMEOUT);
    }

    /** All tasks start together (latch). A hang fails loudly after the timeout instead of freezing the build. */
    static <T> RunResult<T> run(List<? extends Callable<T>> tasks, Duration timeout) {
        int n = tasks.size();
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch ready = new CountDownLatch(n);
        CountDownLatch start = new CountDownLatch(1);
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            List<Future<Result<T>>> futures = new ArrayList<>();
            for (Callable<T> task : tasks) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    try {
                        return new Result<>(Outcome.OK, task.call(), null);
                    } catch (Throwable e) {
                        return new Result<>(Outcome.classify(e), null, e);
                    }
                }));
            }
            if (!ready.await(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                throw new AssertionError("threads did not all start within " + timeout);
            }
            long t0 = System.nanoTime();
            start.countDown();
            List<Result<T>> results = new ArrayList<>();
            for (Future<Result<T>> f : futures) {
                long left = Math.max(0, deadline - System.nanoTime());
                try {
                    results.add(f.get(left, TimeUnit.NANOSECONDS));
                } catch (TimeoutException e) {
                    throw new AssertionError("run did not finish within " + timeout + " (hang or deadlock)");
                } catch (ExecutionException e) {
                    throw new AssertionError("runner failure", e);
                }
            }
            return new RunResult<>(results, (System.nanoTime() - t0) / 1_000_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", e);
        } finally {
            pool.shutdownNow();
        }
    }
}