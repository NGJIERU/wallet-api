package com.wallet.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Runs the relay on one dedicated thread: a full batch runs again immediately, an idle relay waits poll-interval,
 * a failure backs off (doubling up to backoff-max) and logs ONE warning with counts. Stops on graceful shutdown.
 * Only exists when app.outbox.relay.enabled=true, so tests that assert unpublished rows are not disturbed.
 */
@Component
@ConditionalOnProperty(name = "app.outbox.relay.enabled", havingValue = "true")
public class OutboxRelayScheduler implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final OutboxRelay relay;
    private final Duration pollInterval;
    private final Duration backoffMin;
    private final Duration backoffMax;
    private final int batchSize;
    private volatile Thread thread;
    private volatile boolean running;

    public OutboxRelayScheduler(OutboxRelay relay,
                                @Value("${app.outbox.relay.poll-interval:500ms}") Duration pollInterval,
                                @Value("${app.outbox.relay.backoff-min:1s}") Duration backoffMin,
                                @Value("${app.outbox.relay.backoff-max:30s}") Duration backoffMax,
                                @Value("${app.outbox.relay.batch-size:100}") int batchSize) {
        this.relay = relay;
        this.pollInterval = pollInterval;
        this.backoffMin = backoffMin;
        this.backoffMax = backoffMax;
        this.batchSize = batchSize;
    }

    /** One loop step; returns how long to wait before the next one (zero = run again now). Package-private for tests. */
    Duration step(Duration currentBackoff, Duration[] nextBackoff) {
        OutboxRelay.BatchResult r = relay.publishBatch();
        if (r.failure() != null) {
            Duration wait = currentBackoff == null ? backoffMin : min(currentBackoff.multipliedBy(2), backoffMax);
            nextBackoff[0] = wait;
            log.warn("outbox relay failed: published={} selected={} headRow={} consecutiveFailures={} retryIn={}ms cause={}",
                    r.published(), r.selected(), r.failedRow() == null ? "none" : r.failedRow().id(),
                    r.consecutiveFailures(), wait.toMillis(), r.failure().toString());
            return wait;
        }
        nextBackoff[0] = null;
        return r.selected() >= batchSize ? Duration.ZERO : pollInterval;
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(() -> {
            Duration backoff = null;
            Duration[] next = new Duration[1];
            while (running) {
                Duration wait;
                try {
                    wait = step(backoff, next);
                } catch (RuntimeException e) {
                    log.warn("outbox relay step threw: {}", e.toString());
                    next[0] = backoffMin;
                    wait = backoffMin;
                }
                backoff = next[0];
                if (!wait.isZero()) {
                    try {
                        Thread.sleep(wait.toMillis());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }, "outbox-relay");
        thread.setDaemon(true);
        thread.start();
        log.info("outbox relay started (poll {} ms, batch {})", pollInterval.toMillis(), batchSize);
    }

    @Override
    public synchronized void stop() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
