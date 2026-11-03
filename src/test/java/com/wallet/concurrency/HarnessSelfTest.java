package com.wallet.concurrency;

import com.wallet.account.InsufficientFundsException;
import com.wallet.common.ContentionException;
import com.wallet.idempotency.IdempotencyConflictException;
import com.wallet.idempotency.IdempotencyKeyReuseException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("concurrency")
class HarnessSelfTest {

    @Test
    void classifiesTheKnownBusyAndUnexpectedCases() {
        assertThat(Outcome.classify(new IdempotencyConflictException())).isEqualTo(Outcome.BUSY);
        assertThat(Outcome.classify(new ContentionException(new RuntimeException()))).isEqualTo(Outcome.BUSY);
        assertThat(Outcome.classify(new CannotAcquireLockException("x", new SQLException("t", "55P03"))))
                .isEqualTo(Outcome.BUSY);
        assertThat(Outcome.classify(new CannotAcquireLockException("x", new SQLException("t", "40P01"))))
                .isEqualTo(Outcome.UNEXPECTED);
        assertThat(Outcome.classify(new IllegalStateException("boom"))).isEqualTo(Outcome.UNEXPECTED);
        assertThat(Outcome.classify(new InsufficientFundsException())).isEqualTo(Outcome.INSUFFICIENT);
        assertThat(Outcome.classify(new IdempotencyKeyReuseException())).isEqualTo(Outcome.KEY_REUSE);
    }

    @Test
    void runnerCountsOutcomesPerTask() {
        List<Callable<Integer>> tasks = List.of(
                () -> 1, () -> 2, () -> 3,
                () -> { throw new IdempotencyConflictException(); },
                () -> { throw new IllegalStateException("boom"); });

        ConcurrentRunner.RunResult<Integer> r = ConcurrentRunner.run(tasks);
        r.log("self-test");

        assertThat(r.count(Outcome.OK)).isEqualTo(3);
        assertThat(r.count(Outcome.BUSY)).isEqualTo(1);
        assertThat(r.count(Outcome.UNEXPECTED)).isEqualTo(1);
        assertThatThrownBy(r::assertNoUnexpected).isInstanceOf(AssertionError.class);
    }

    @Test
    void aHungTaskFailsLoudlyInsteadOfFreezing() {
        CountDownLatch never = new CountDownLatch(1);
        List<Callable<Void>> tasks = List.of(() -> { never.await(); return null; });

        assertThatThrownBy(() -> ConcurrentRunner.run(tasks, Duration.ofMillis(500)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("did not finish");
    }
}