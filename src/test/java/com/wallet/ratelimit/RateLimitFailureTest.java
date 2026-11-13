package com.wallet.ratelimit;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * LF1 to LF6: Redis paused, stopped and restarted under the limiter-on context (money capacity 5, negligible refill).
 * Same context and container as the H tests. Every test restores Redis in a finally and uses fresh users.
 * The 1 s bound is a test bound, not a performance claim.
 */
class RateLimitFailureTest extends AbstractRateLimitIntegrationTest {

    private static final long BOUND_MS = 1_000;

    @Autowired RateLimiter limiter;

    private final Logger outageLogger = (Logger) LoggerFactory.getLogger(OutageLog.class);
    private final ListAppender<ILoggingEvent> events = new ListAppender<>();
    private Level previousLevel;

    private record Timed(int status, long ms, String body) {
    }

    @BeforeEach
    void healthyStart() {
        LimiterRedis.ensureRunning();
        awaitRecovered(Duration.ofSeconds(30)); // any earlier outage state ends here, before we start counting logs
        previousLevel = outageLogger.getLevel();
        outageLogger.setLevel(Level.INFO);
        events.start();
        outageLogger.addAppender(events);
    }

    @AfterEach
    void restore() {
        outageLogger.detachAppender(events);
        outageLogger.setLevel(previousLevel);
        LimiterRedis.ensureRunning();
    }

    private long awaitRecovered(Duration deadline) {
        long t0 = System.nanoTime();
        Awaitility.await().atMost(deadline).pollInterval(Duration.ofMillis(50))
                .until(() -> !limiter.tryAcquire("probe:" + UUID.randomUUID(), 1, 1.0).degraded());
        return (System.nanoTime() - t0) / 1_000_000;
    }

    private long countOf(Level level) {
        return events.list.stream().filter(e -> e.getLevel() == level).count();
    }

    private long count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }

    private long[] rows() {
        return new long[]{count("ledger_entries"), count("idempotency_keys"), count("outbox_events")};
    }

    private long ledgerRows(long accountId) {
        return jdbc.sql("SELECT count(*) FROM ledger_entries WHERE account_id = ?").param(accountId)
                .query(Long.class).single();
    }

    private Timed timedDeposit(String token, long accountId) throws Exception {
        long t0 = System.nanoTime();
        MvcResult r = deposit(token, accountId, key()).andReturn();
        long ms = (System.nanoTime() - t0) / 1_000_000;
        return new Timed(r.getResponse().getStatus(), ms, r.getResponse().getContentAsString());
    }

    private static String stats(List<Timed> list) {
        List<Long> s = list.stream().map(Timed::ms).sorted().toList();
        return "min=" + s.get(0) + "ms median=" + s.get(s.size() / 2) + "ms max=" + s.get(s.size() - 1) + "ms";
    }

    private void assertHealthUp() throws Exception {
        mvc.perform(MockMvcRequestBuilders.get("/actuator/health"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
    }

    private void assertEnforced() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        for (int i = 0; i < 5; i++) {
            deposit(token, accountId, key()).andExpect(status().isCreated());
        }
        deposit(token, accountId, key()).andExpect(status().isTooManyRequests());
    }

    private void assertAllSucceededWithinBound(List<Timed> results) {
        for (Timed t : results) {
            assertThat(t.status()).as("deposit during outage").isEqualTo(201);
            assertThat(t.ms()).as("request time during outage (test bound)").isLessThan(BOUND_MS);
        }
    }

    private List<Timed> outageDeposits(String token, long accountId, int n) throws Exception {
        List<Timed> results = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            results.add(timedDeposit(token, accountId));
        }
        return results;
    }

    // LF1
    @Test
    void lf1_pausedRedis_moreDepositsThanCapacityAllSucceed_oneWarn() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        deposit(token, accountId, key()).andExpect(status().isCreated()); // warm-up: the connection exists
        long[] before = rows();
        List<Timed> results;
        try {
            LimiterRedis.pause();
            results = outageDeposits(token, accountId, 8); // 9 requests in total against a capacity of 5
            assertHealthUp();
        } finally {
            LimiterRedis.ensureRunning();
        }
        long[] after = rows();
        System.out.println("[LIMITER LF1 paused deposits=8 statuses=" + results.stream().map(Timed::status).toList()
                + " " + stats(results) + " warns=" + countOf(Level.WARN) + "]");
        assertAllSucceededWithinBound(results);
        assertThat(countOf(Level.WARN)).as("one WARN for the outage window").isEqualTo(1);
        assertThat(after[0] - before[0]).as("ledger rows").isEqualTo(8);
        assertThat(after[1] - before[1]).as("idempotency rows").isEqualTo(8);
        assertThat(after[2] - before[2]).as("outbox rows").isEqualTo(8);
    }

    // LF2
    @Test
    void lf2_pausedRedis_twentyParallelDepositsFromSeveralUsers() throws Exception {
        List<String> tokens = new ArrayList<>();
        List<Long> accounts = new ArrayList<>();
        for (int u = 0; u < 4; u++) {
            String token = registerAndLogin();
            tokens.add(token);
            accounts.add(createAccount(token));
        }
        ExecutorService pool = Executors.newFixedThreadPool(20);
        List<Timed> results = new ArrayList<>();
        try {
            LimiterRedis.pause();
            List<Future<Timed>> futures = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                int u = i % 4;
                futures.add(pool.submit(() -> timedDeposit(tokens.get(u), accounts.get(u))));
            }
            for (Future<Timed> f : futures) {
                results.add(f.get(60, TimeUnit.SECONDS));
            }
            assertHealthUp();
        } finally {
            pool.shutdownNow();
            LimiterRedis.ensureRunning();
        }
        System.out.println("[LIMITER LF2 paused parallel=20 statuses201=" + results.stream().filter(t -> t.status() == 201).count()
                + " " + stats(results) + "]");
        assertAllSucceededWithinBound(results);
    }

    // LF3
    @Test
    void lf3_unpauseRecoversWithoutRestart_andEnforcementResumes() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        deposit(token, accountId, key()).andExpect(status().isCreated());
        try {
            LimiterRedis.pause();
            assertAllSucceededWithinBound(outageDeposits(token, accountId, 3));
        } finally {
            LimiterRedis.ensureRunning();
        }
        long recoveryMs = awaitRecovered(Duration.ofSeconds(15));
        System.out.println("[LIMITER LF3 unpause recovery=" + recoveryMs + "ms warns=" + countOf(Level.WARN)
                + " infos=" + countOf(Level.INFO) + "]");
        assertThat(countOf(Level.INFO)).as("one INFO on recovery").isEqualTo(1);
        assertEnforced();
    }

    // LF4
    @Test
    void lf4_stoppedRedis_depositsSucceedWithinBound() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        deposit(token, accountId, key()).andExpect(status().isCreated()); // connection established, then dropped by stop
        List<Timed> results;
        try {
            LimiterRedis.stop();
            results = outageDeposits(token, accountId, 8);
            assertHealthUp();
        } finally {
            LimiterRedis.ensureRunning();
        }
        System.out.println("[LIMITER LF4 stopped deposits=8 " + stats(results) + " command timeout="
                + RedisClientSettings.COMMAND_TIMEOUT.toMillis() + "ms warns=" + countOf(Level.WARN) + "]");
        assertAllSucceededWithinBound(results);
        assertThat(countOf(Level.WARN)).as("one WARN for the outage window").isEqualTo(1);
        awaitRecovered(Duration.ofSeconds(30)); // leave the context healthy for other classes
    }

    // LF5
    @Test
    void lf5_startAgainOnTheSamePort_recoversWithoutApplicationRestart() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        deposit(token, accountId, key()).andExpect(status().isCreated());
        try {
            LimiterRedis.stop();
            assertAllSucceededWithinBound(outageDeposits(token, accountId, 3));
        } finally {
            LimiterRedis.ensureRunning(); // start on the same fixed port
        }
        long recoveryMs = awaitRecovered(Duration.ofSeconds(30));
        System.out.println("[LIMITER LF5 restart recovery=" + recoveryMs + "ms (deadline 30000ms) warns=" + countOf(Level.WARN)
                + " infos=" + countOf(Level.INFO) + "]");
        assertThat(countOf(Level.INFO)).as("one INFO on recovery").isEqualTo(1);
        assertEnforced(); // buckets were reset by the restart; a fresh user gets exactly the capacity
    }

    // LF6
    @Test
    @SuppressWarnings("unchecked")
    void lf6_degradedAndRecoveredRequestsGiveTheSameBusinessResult() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        deposit(token, accountId, key()).andExpect(status().isCreated()); // warm-up, ledger row 1
        Timed degraded;
        try {
            LimiterRedis.pause();
            degraded = timedDeposit(token, accountId); // ledger row 2
        } finally {
            LimiterRedis.ensureRunning();
        }
        awaitRecovered(Duration.ofSeconds(15));
        Timed recovered = timedDeposit(token, accountId); // ledger row 3
        Map<String, Object> a = mapper.readValue(degraded.body(), Map.class);
        Map<String, Object> b = mapper.readValue(recovered.body(), Map.class);
        System.out.println("[LIMITER LF6 degraded status=" + degraded.status() + " fields=" + a.keySet()
                + " recovered status=" + recovered.status() + " fields=" + b.keySet()
                + " balanceAfter=" + a.get("balanceAfter") + "/" + b.get("balanceAfter") + "]");
        assertThat(degraded.status()).isEqualTo(201);
        assertThat(recovered.status()).isEqualTo(201);
        assertThat(a.keySet()).containsExactlyInAnyOrderElementsOf(b.keySet());
        assertThat(((Number) a.get("balanceAfter")).longValue()).isEqualTo(200);
        assertThat(((Number) b.get("balanceAfter")).longValue()).isEqualTo(300);
        assertThat(ledgerRows(accountId)).as("one ledger row per deposit, no duplicates").isEqualTo(3);
    }
}
