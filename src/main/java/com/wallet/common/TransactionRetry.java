package com.wallet.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Re-runs a whole transaction when Postgres kills it as a deadlock victim (40P01) or reports a
 * serialization failure (40001). The action passed in must start its own transaction on every call,
 * so it has to go through a Spring proxy and must not already run inside a transaction.
 * A lock timeout (55P03) is NOT retried here: the lock holder is still busy, the client retries later.
 */
public final class TransactionRetry {

    private static final Logger log = LoggerFactory.getLogger(TransactionRetry.class);

    private final int maxAttempts;
    private final long baseBackoffMs;
    private final LongConsumer sleeper;

    public TransactionRetry(int maxAttempts, long baseBackoffMs) {
        this(maxAttempts, baseBackoffMs, TransactionRetry::sleep);
    }

    // the sleeper is injectable so tests do not really wait
    TransactionRetry(int maxAttempts, long baseBackoffMs, LongConsumer sleeper) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        this.maxAttempts = maxAttempts;
        this.baseBackoffMs = baseBackoffMs;
        this.sleeper = sleeper;
    }

    public <T> T run(Supplier<T> action) {
        for (int attempt = 1; ; attempt++) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                if (!PgErrors.isRetryable(e)) {
                    throw e;
                }
                if (attempt >= maxAttempts) {
                    throw new ContentionException(e);
                }
                long delay = (baseBackoffMs << (attempt - 1)) + ThreadLocalRandom.current().nextLong(baseBackoffMs + 1);
                log.warn("Retryable database failure (attempt {} of {}), retrying in {} ms: {}",
                        attempt, maxAttempts, delay, e.toString());
                sleeper.accept(delay);
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}