package com.wallet.ratelimit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** No Spring context: the script and the service against a real, shared Redis. */
class RateLimiterTest {

    /** Positive, but so small that a bucket cannot gain one token during a test. */
    private static final double NEGLIGIBLE_RATE = 0.0001;
    private static final int REPEATS = 5;

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;
    private static RateLimiter limiter;

    @BeforeAll
    static void connect() {
        factory = RedisClientSettings.connectionFactory(SharedRedis.host(), SharedRedis.port());
        template = new StringRedisTemplate(factory);
        template.afterPropertiesSet();
        limiter = new RateLimiter(template);
        // warm-up: connection and script load happen here, not inside the timed race
        assertThat(limiter.tryAcquire(key("warmup"), 1, NEGLIGIBLE_RATE).degraded()).isFalse();
    }

    @AfterAll
    static void close() {
        factory.destroy();
    }

    private static String key(String name) {
        return "test:" + name + ":" + UUID.randomUUID();
    }

    private record Counts(int allowed, int denied, int degraded) {
    }

    /** All threads of all keys are released by one latch; returns the counts per key. */
    private static Map<String, Counts> race(List<String> keys, long capacity, int threadsPerKey) throws Exception {
        int total = keys.size() * threadsPerKey;
        CountDownLatch ready = new CountDownLatch(total);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(total);
        try {
            Map<String, List<Future<RateLimitDecision>>> futures = new LinkedHashMap<>();
            for (String k : keys) {
                List<Future<RateLimitDecision>> list = new ArrayList<>();
                for (int i = 0; i < threadsPerKey; i++) {
                    list.add(pool.submit(() -> {
                        ready.countDown();
                        start.await();
                        return limiter.tryAcquire(k, capacity, NEGLIGIBLE_RATE);
                    }));
                }
                futures.put(k, list);
            }
            assertThat(ready.await(30, TimeUnit.SECONDS)).as("all threads ready").isTrue();
            start.countDown();
            Map<String, Counts> result = new LinkedHashMap<>();
            for (var e : futures.entrySet()) {
                int allowed = 0, denied = 0, degraded = 0;
                for (Future<RateLimitDecision> f : e.getValue()) {
                    RateLimitDecision d = f.get(30, TimeUnit.SECONDS);
                    if (d.degraded()) degraded++;
                    else if (d.allowed()) allowed++;
                    else denied++;
                }
                result.put(e.getKey(), new Counts(allowed, denied, degraded));
            }
            return result;
        } finally {
            pool.shutdownNow();
        }
    }

    // S1
    @ParameterizedTest(name = "S1 capacity={0}, threads={1}")
    @CsvSource({"1,32", "10,50", "100,64", "100,200"})
    void s1_exactlyCapacityAllowed(int capacity, int threads) throws Exception {
        for (int run = 1; run <= REPEATS; run++) {
            String k = key("s1");
            Counts c = race(List.of(k), capacity, threads).get(k);
            System.out.println("[LIMITER S1 C=" + capacity + " N=" + threads + " run=" + run
                    + " allowed=" + c.allowed() + " denied=" + c.denied() + " degraded=" + c.degraded() + "]");
            assertThat(c.degraded()).as("no Redis failure during the race").isZero();
            // with fewer callers than tokens everyone is allowed; otherwise exactly C
            assertThat(c.allowed()).isEqualTo(Math.min(capacity, threads));
            assertThat(c.denied()).isEqualTo(Math.max(0, threads - capacity));
        }
    }

    // S1 (several keys at once)
    @Test
    void s1_severalKeysAtOnce() throws Exception {
        List<String> keys = List.of(key("s1m"), key("s1m"), key("s1m"));
        for (int run = 1; run <= REPEATS; run++) {
            List<String> fresh = keys.stream().map(k -> k + ":" + System.nanoTime()).toList();
            Map<String, Counts> result = race(fresh, 10, 30);
            for (var e : result.entrySet()) {
                Counts c = e.getValue();
                System.out.println("[LIMITER S1 multi C=10 N=30/key keys=3 run=" + run
                        + " allowed=" + c.allowed() + " denied=" + c.denied() + " degraded=" + c.degraded() + "]");
                assertThat(c.degraded()).isZero();
                assertThat(c.allowed()).isEqualTo(10);
            }
        }
    }

    // S2
    @Test
    void s2_sequentialDrain() {
        String k = key("s2");
        for (int expectedRemaining = 4; expectedRemaining >= 0; expectedRemaining--) {
            RateLimitDecision d = limiter.tryAcquire(k, 5, NEGLIGIBLE_RATE);
            assertThat(d.allowed()).isTrue();
            assertThat(d.remaining()).isEqualTo(expectedRemaining);
            assertThat(d.retryAfterMs()).isZero();
        }
        RateLimitDecision denied = limiter.tryAcquire(k, 5, NEGLIGIBLE_RATE);
        assertThat(denied.allowed()).isFalse();
        assertThat(denied.remaining()).isZero();
        assertThat(denied.retryAfterMs()).isPositive();
        assertThat(denied.degraded()).isFalse();
    }

    // S3
    @Test
    void s3_refill() {
        String k = key("s3");
        double rate = 5.0; // one token per 200 ms
        assertThat(limiter.tryAcquire(k, 1, rate).allowed()).isTrue();
        RateLimitDecision denied = limiter.tryAcquire(k, 1, rate);
        assertThat(denied.allowed()).isFalse();
        assertThat(denied.retryAfterMs()).isPositive().isLessThanOrEqualTo(200 + 50); // interval + 50 ms margin
        Awaitility.await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(20))
                .until(() -> limiter.tryAcquire(k, 1, rate).allowed());
    }

    // S4
    @Test
    void s4_keysAreIsolated() {
        String a = key("s4a");
        String b = key("s4b");
        assertThat(limiter.tryAcquire(a, 1, NEGLIGIBLE_RATE).allowed()).isTrue();
        assertThat(limiter.tryAcquire(a, 1, NEGLIGIBLE_RATE).allowed()).isFalse();
        assertThat(limiter.tryAcquire(b, 1, NEGLIGIBLE_RATE).allowed()).isTrue();
    }

    // S5
    @Test
    void s5_ttl() {
        String slow = key("s5slow");
        limiter.tryAcquire(slow, 10, 0.5); // time to full = 20 s
        Long ttl = template.getExpire(slow, TimeUnit.MILLISECONDS);
        assertThat(ttl).isNotNull().isPositive().isLessThanOrEqualTo(20_000L);

        String fast = key("s5fast");
        limiter.tryAcquire(fast, 1, 10.0); // time to full = 100 ms, so the 1 s minimum applies
        assertThat(template.getExpire(fast, TimeUnit.MILLISECONDS)).isPositive().isLessThanOrEqualTo(1_000L);
        Awaitility.await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(50))
                .until(() -> !Boolean.TRUE.equals(template.hasKey(fast)));
    }

    // S6
    @Test
    void s6_scriptFlushThenCall() {
        String k = key("s6");
        assertThat(limiter.tryAcquire(k, 3, NEGLIGIBLE_RATE).allowed()).isTrue();
        template.execute((RedisCallback<Object>) c -> c.execute("SCRIPT", "FLUSH".getBytes(StandardCharsets.UTF_8)));
        RateLimitDecision d = limiter.tryAcquire(k, 3, NEGLIGIBLE_RATE);
        assertThat(d.degraded()).isFalse();
        assertThat(d.allowed()).isTrue();
        assertThat(d.remaining()).isEqualTo(1);
    }

    // S7
    @Test
    void s7_invalidArgumentsRejectedBeforeRedis() {
        StringRedisTemplate untouched = mock(StringRedisTemplate.class);
        RateLimiter strict = new RateLimiter(untouched);
        assertThatThrownBy(() -> strict.tryAcquire("k", 0, 1.0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strict.tryAcquire("k", -5, 1.0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strict.tryAcquire("k", 1, 0.0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strict.tryAcquire("k", 1, -1.0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strict.tryAcquire("k", 1, Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strict.tryAcquire("k", 1, Double.POSITIVE_INFINITY)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strict.tryAcquire("", 1, 1.0)).isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(untouched);
    }

    // S8
    @Test
    void s8_failOpenAgainstClosedPort() throws Exception {
        int deadPort;
        try (ServerSocket s = new ServerSocket(0)) {
            deadPort = s.getLocalPort();
        } // closed again: nothing listens here
        LettuceConnectionFactory deadFactory = RedisClientSettings.connectionFactory("localhost", deadPort);
        try {
            StringRedisTemplate deadTemplate = new StringRedisTemplate(deadFactory);
            deadTemplate.afterPropertiesSet();
            RateLimiter deadLimiter = new RateLimiter(deadTemplate);

            long t1 = System.nanoTime();
            RateLimitDecision first = deadLimiter.tryAcquire("s8", 1, 1.0);
            long firstMs = (System.nanoTime() - t1) / 1_000_000;
            long t2 = System.nanoTime();
            RateLimitDecision second = deadLimiter.tryAcquire("s8", 1, 1.0);
            long secondMs = (System.nanoTime() - t2) / 1_000_000;

            System.out.println("[LIMITER S8 closed port first=" + firstMs + "ms second=" + secondMs
                    + "ms allowed=" + first.allowed() + "/" + second.allowed()
                    + " degraded=" + first.degraded() + "/" + second.degraded() + "]");
            assertThat(first.allowed()).isTrue();
            assertThat(first.degraded()).isTrue();
            assertThat(second.allowed()).isTrue();
            assertThat(second.degraded()).isTrue();
            // test bounds, not a benchmark: the first call includes lazy client start-up
            assertThat(firstMs).isLessThan(2_000);
            assertThat(secondMs).isLessThan(1_000);
        } finally {
            deadFactory.destroy();
        }
    }
}
