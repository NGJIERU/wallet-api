package com.wallet.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** RT10: no Spring Boot application context, only a tiny runner with a stub relay. */
class OutboxRelaySchedulerTest {

    private static final class StubRelay extends OutboxRelay {
        final AtomicInteger calls = new AtomicInteger();
        volatile BatchResult next = new BatchResult(0, 0, null, null, 0);

        StubRelay() {
            super(null, null, 100);
        }

        @Override
        public BatchResult publishBatch() {
            calls.incrementAndGet();
            return next;
        }
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(OutboxRelayScheduler.class)
            .withBean("conversionService", org.springframework.core.convert.ConversionService.class,
                    ApplicationConversionService::getSharedInstance)
            .withBean(OutboxRelay.class, StubRelay::new);

    @Test
    void theSchedulerBeanIsAbsentWhenTheSwitchIsOffOrUnset() {
        runner.withPropertyValues("app.outbox.relay.enabled=false")
                .run(ctx -> assertThat(ctx).doesNotHaveBean(OutboxRelayScheduler.class));
        runner.run(ctx -> assertThat(ctx).doesNotHaveBean(OutboxRelayScheduler.class));
    }

    @Test
    void theSchedulerRunsTheRelayWhenTheSwitchIsOn() {
        runner.withPropertyValues("app.outbox.relay.enabled=true", "app.outbox.relay.poll-interval=10ms")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(OutboxRelayScheduler.class);
                    StubRelay relay = (StubRelay) ctx.getBean(OutboxRelay.class);
                    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                    while (relay.calls.get() < 2 && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                    assertThat(relay.calls.get())
                            .as("calls=" + relay.calls.get() + " running=" + ctx.getBean(OutboxRelayScheduler.class).isRunning())
                            .isGreaterThanOrEqualTo(2);
                });
    }

    @Test
    void aFailureBacksOffDoublingUpToTheMaximumAndAFullBatchRunsAgainImmediately() {
        StubRelay relay = new StubRelay();
        OutboxRelayScheduler s = new OutboxRelayScheduler(relay, Duration.ofMillis(500),
                Duration.ofSeconds(1), Duration.ofSeconds(4), 100);
        Duration[] next = new Duration[1];
        OutboxRelay.Row row = new OutboxRelay.Row(7, "1", "t", "{}");
        relay.next = new OutboxRelay.BatchResult(3, 0, row, new IllegalStateException("x"), 1);

        Duration w1 = s.step(null, next);
        Duration w2 = s.step(next[0], next);
        Duration w3 = s.step(next[0], next);
        Duration w4 = s.step(next[0], next);
        assertThat(java.util.List.of(w1, w2, w3, w4)).containsExactly(
                Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4), Duration.ofSeconds(4));

        relay.next = new OutboxRelay.BatchResult(100, 100, null, null, 0);
        assertThat(s.step(next[0], next)).isEqualTo(Duration.ZERO);
        assertThat(next[0]).as("backoff resets after a success").isNull();
        relay.next = new OutboxRelay.BatchResult(0, 0, null, null, 0);
        assertThat(s.step(null, next)).isEqualTo(Duration.ofMillis(500));
    }
}
