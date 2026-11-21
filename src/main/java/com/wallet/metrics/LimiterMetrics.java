package com.wallet.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Limiter decision counter (Q-D). All scope x outcome series are registered at construction, so they exist at 0.
 * Tags: only "scope" and "outcome", fixed value sets; never a user, IP or key.
 */
@Component
public class LimiterMetrics {

    public static final List<String> SCOPES = List.of("auth", "money", "default");
    public static final List<String> OUTCOMES = List.of("allowed", "denied", "degraded");

    private final Map<String, Counter> counters = new HashMap<>();

    public LimiterMetrics(MeterRegistry registry) {
        for (String scope : SCOPES) {
            for (String outcome : OUTCOMES) {
                counters.put(scope + "/" + outcome, Counter.builder("wallet.ratelimit.decisions")
                        .tag("scope", scope).tag("outcome", outcome)
                        .description("Rate limiter decisions").register(registry));
            }
        }
    }

    public void record(String scope, String outcome) {
        Counter c = counters.get(scope + "/" + outcome);
        if (c != null) {
            c.increment();
        }
    }
}
