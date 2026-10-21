package com.wallet.common;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionRetryTest {

    private final List<Long> sleeps = new ArrayList<>();
    private final TransactionRetry retry = new TransactionRetry(3, 10, sleeps::add);

    private static RuntimeException failure(String sqlState) {
        return new RuntimeException("wrapped", new SQLException("boom", sqlState));
    }

    @Test
    void retriesADeadlockAndReturnsTheLaterSuccess() {
        AtomicInteger calls = new AtomicInteger();

        String result = retry.run(() -> {
            if (calls.incrementAndGet() < 3) {
                throw failure("40P01");
            }
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(3);
        assertThat(sleeps).hasSize(2);
    }

    @Test
    void retriesASerializationFailure() {
        AtomicInteger calls = new AtomicInteger();

        String result = retry.run(() -> {
            if (calls.incrementAndGet() < 2) {
                throw failure("40001");
            }
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(2);
    }

    @Test
    void doesNotRetryALockTimeout() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> retry.run(() -> {
            calls.incrementAndGet();
            throw failure("55P03");
        })).hasMessage("wrapped");

        assertThat(calls).hasValue(1);
        assertThat(sleeps).isEmpty();
    }

    @Test
    void doesNotRetryOrdinaryErrors() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> retry.run(() -> {
            calls.incrementAndGet();
            throw new IllegalStateException("not a database conflict");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(calls).hasValue(1);
    }

    @Test
    void givesUpAfterTheLastAttemptWithAContentionException() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> retry.run(() -> {
            calls.incrementAndGet();
            throw failure("40P01");
        })).isInstanceOf(ContentionException.class)
                .hasCauseInstanceOf(RuntimeException.class);

        assertThat(calls).hasValue(3);
        assertThat(sleeps).hasSize(2); // no sleep after the final attempt
    }

    @Test
    void backoffGrowsWithEachAttempt() {
        assertThatThrownBy(() -> retry.run(() -> {
            throw failure("40P01");
        })).isInstanceOf(ContentionException.class);

        assertThat(sleeps.get(0)).isBetween(10L, 20L); // 10 + jitter 0..10
        assertThat(sleeps.get(1)).isBetween(20L, 30L); // 20 + jitter 0..10
    }
}