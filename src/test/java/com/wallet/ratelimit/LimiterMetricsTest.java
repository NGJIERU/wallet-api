package com.wallet.ratelimit;

import com.wallet.metrics.LimiterMetrics;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** LM1 to LM3 in the existing limiter-on context. The registry is shared, so counters are asserted as deltas. */
class LimiterMetricsTest extends AbstractRateLimitIntegrationTest {

    private static final String NAME = "wallet.ratelimit.decisions";

    @Autowired MeterRegistry registry;
    @Autowired RateLimiter limiter;

    private double c(String scope, String outcome) {
        return registry.get(NAME).tag("scope", scope).tag("outcome", outcome).counter().count();
    }

    // LM1
    @Test
    void lm1_fiveAllowedThenOneDeniedForTheMoneyScope() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        double allowed0 = c("money", "allowed");
        double denied0 = c("money", "denied");
        double degraded0 = c("money", "degraded");
        for (int i = 0; i < 5; i++) {
            deposit(token, accountId, key()).andExpect(status().isCreated());
        }
        deposit(token, accountId, key()).andExpect(status().isTooManyRequests());
        assertThat(c("money", "allowed") - allowed0).isEqualTo(5.0);
        assertThat(c("money", "denied") - denied0).isEqualTo(1.0);
        assertThat(c("money", "degraded") - degraded0).isZero();
    }

    // LM2
    @Test
    void lm2_pausedRedisCountsDegradedAndNeverDenied() throws Exception {
        LimiterRedis.ensureRunning();
        awaitRecovered();
        String token = registerAndLogin();
        long accountId = createAccount(token);
        deposit(token, accountId, key()).andExpect(status().isCreated()); // warm-up: the connection exists
        double degraded0 = c("money", "degraded");
        double denied0 = c("money", "denied");
        LimiterRedis.pause();
        try {
            deposit(token, accountId, key()).andExpect(status().isCreated());
            deposit(token, accountId, key()).andExpect(status().isCreated());
        } finally {
            LimiterRedis.unpause();
            LimiterRedis.ensureRunning();
            awaitRecovered();
        }
        assertThat(c("money", "degraded") - degraded0).isEqualTo(2.0);
        assertThat(c("money", "denied") - denied0).isZero();
    }

    private void awaitRecovered() {
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50))
                .until(() -> !limiter.tryAcquire("probe:" + UUID.randomUUID(), 1, 1.0).degraded());
    }

    // LM3: cardinality guard over every wallet.* meter
    @Test
    void lm3_everyWalletMeterUsesOnlyTheApprovedNamesTagKeysAndValues() {
        Set<String> approved = Set.of("wallet.outbox.unpublished", "wallet.outbox.oldest.age.seconds",
                "wallet.relay.published", "wallet.relay.failed", "wallet.consumer.events", NAME);
        Set<String> outcomes = Set.of("processed", "duplicate", "dead_lettered", "skipped", "allowed", "denied", "degraded");
        Set<String> scopes = Set.copyOf(LimiterMetrics.SCOPES);
        List<Meter> wallet = registry.getMeters().stream().filter(m -> m.getId().getName().startsWith("wallet.")).toList();
        assertThat(wallet).isNotEmpty();
        for (Meter m : wallet) {
            assertThat(approved).as("meter name").contains(m.getId().getName());
            for (Tag t : m.getId().getTags()) {
                assertThat(t.getKey()).as("tag key of " + m.getId().getName()).isIn("outcome", "scope");
                if (t.getKey().equals("outcome")) {
                    assertThat(outcomes).as("outcome value").contains(t.getValue());
                } else {
                    assertThat(scopes).as("scope value").contains(t.getValue());
                }
            }
        }
        assertThat(wallet.stream().map(m -> m.getId().getName()).distinct().toList()).containsExactlyInAnyOrderElementsOf(approved);
        assertThat(wallet.stream().filter(m -> m.getId().getName().equals(NAME)).count()).as("limiter series").isEqualTo(9);
    }
}
