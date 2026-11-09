package com.wallet.notification;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * NotificationConsumerLifecycle with a stubbed registry and container, no Spring context. The consumer is off in the
 * integration tests, so this is the only automated cover for the two defects found by the Docker checks in step 5.4
 * (an unresolvable broker aborting start-up; a failed first start leaving the container marked as running).
 */
class NotificationConsumerLifecycleTest {

    private static final Duration BACKOFF_MIN = Duration.ofMillis(10);
    private static final Duration BACKOFF_MAX = Duration.ofMillis(40);
    private static final long DEADLINE_SECONDS = 5;
    private static final String THREAD_NAME = "notification-consumer-starter";

    private NotificationConsumerLifecycle lifecycle;

    /** A container whose first {@code failingStarts} calls to start() throw after marking it as running, like Spring's. */
    private static final class FakeContainer {
        final MessageListenerContainer mock = mock(MessageListenerContainer.class);
        final AtomicBoolean running = new AtomicBoolean();
        final AtomicInteger startCalls = new AtomicInteger();
        final AtomicInteger stopCalls = new AtomicInteger();
        final CountDownLatch succeeded = new CountDownLatch(1);
        final CountDownLatch attempts;

        FakeContainer(int failingStarts, int attemptsToAwait) {
            this.attempts = new CountDownLatch(attemptsToAwait);
            when(mock.isRunning()).thenAnswer(i -> running.get());
            doAnswer(i -> {
                int n = startCalls.incrementAndGet();
                running.set(true); // a failed start leaves the container marked as running
                attempts.countDown();
                if (n <= failingStarts) {
                    throw new IllegalStateException("broker unresolvable");
                }
                succeeded.countDown();
                return null;
            }).when(mock).start();
            doAnswer(i -> {
                stopCalls.incrementAndGet();
                running.set(false);
                return null;
            }).when(mock).stop();
        }
    }

    private NotificationConsumerLifecycle lifecycleFor(FakeContainer container) {
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getListenerContainer(NotificationListener.LISTENER_ID)).thenReturn(container.mock);
        lifecycle = new NotificationConsumerLifecycle(registry, BACKOFF_MIN, BACKOFF_MAX);
        return lifecycle;
    }

    @AfterEach
    void tearDown() {
        if (lifecycle != null) {
            lifecycle.stop();
        }
    }

    @Test
    void l1_failedFirstStartIsResetAndTheSecondAttemptStartsTheContainer() throws Exception {
        FakeContainer container = new FakeContainer(1, 1);
        lifecycleFor(container).start();

        assertThat(container.succeeded.await(DEADLINE_SECONDS, TimeUnit.SECONDS))
                .as("second attempt must call start() again, not skip it because the container looks running")
                .isTrue();
        assertThat(container.startCalls.get()).isEqualTo(2);
        assertThat(container.running.get()).isTrue();
        assertThat(container.stopCalls.get()).as("the failed start was reset before retrying").isGreaterThanOrEqualTo(1);
    }

    @Test
    void l2_startNeverThrowsOrBlocksTheCallerWhenTheContainerCannotStartAndKeepsRetrying() throws Exception {
        FakeContainer container = new FakeContainer(Integer.MAX_VALUE, 3);
        NotificationConsumerLifecycle l = lifecycleFor(container);

        assertTimeoutPreemptively(Duration.ofSeconds(2),
                () -> assertThatCode(l::start).doesNotThrowAnyException());
        assertThat(l.isRunning()).isTrue();

        assertThat(container.attempts.await(DEADLINE_SECONDS, TimeUnit.SECONDS))
                .as("three attempts: the starter thread keeps retrying instead of giving up")
                .isTrue();
    }

    @Test
    void l3_stopDuringRetriesEndsTheThreadAndLeavesTheContainerStopped() throws Exception {
        FakeContainer container = new FakeContainer(Integer.MAX_VALUE, 2);
        NotificationConsumerLifecycle l = lifecycleFor(container);
        l.start();
        assertThat(container.attempts.await(DEADLINE_SECONDS, TimeUnit.SECONDS)).isTrue();

        l.stop();

        assertThat(l.isRunning()).isFalse();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (THREAD_NAME.equals(t.getName())) {
                t.join(DEADLINE_SECONDS * 1000);
                assertThat(t.isAlive()).as("starter thread must end after stop()").isFalse();
            }
        }
        assertThat(container.running.get()).as("container left stopped").isFalse();
    }
}
