package com.wallet.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Relay and consumer counters (Q-B and Q-C). Every series is registered at construction, so it exists at 0 before the
 * first event. Tags: only "outcome", with a fixed value set; never an id. Micrometer calls do not throw.
 */
@Component
public class EventMetrics {

    public static final List<String> CONSUMER_OUTCOMES = List.of("processed", "duplicate", "dead_lettered", "skipped");

    private final Counter published;
    private final Counter failed;
    private final Map<String, Counter> consumer = new HashMap<>();

    public EventMetrics(MeterRegistry registry) {
        published = Counter.builder("wallet.relay.published").description("Outbox rows marked published").register(registry);
        failed = Counter.builder("wallet.relay.failed").description("Failed relay attempts").register(registry);
        for (String outcome : CONSUMER_OUTCOMES) {
            consumer.put(outcome, Counter.builder("wallet.consumer.events").tag("outcome", outcome)
                    .description("Consumer events by outcome").register(registry));
        }
    }

    public void relayPublished() {
        published.increment();
    }

    public void relayFailed() {
        failed.increment();
    }

    public void consumerEvent(String outcome) {
        Counter c = consumer.get(outcome);
        if (c != null) {
            c.increment();
        }
    }
}
