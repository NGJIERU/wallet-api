package com.wallet.ratelimit;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** No Spring context and no Redis for W1 to W3: a fake clock and a captured logger. W4 uses a closed port. */
class OutageLogTest {

    private static final Duration WINDOW = Duration.ofSeconds(30);

    private static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private final Logger logger = (Logger) LoggerFactory.getLogger(OutageLog.class);
    private final ListAppender<ILoggingEvent> events = new ListAppender<>();
    private Level previousLevel;
    private final MutableClock clock = new MutableClock();
    private final OutageLog outage = new OutageLog(clock, WINDOW);

    @BeforeEach
    void capture() {
        previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        events.start();
        logger.addAppender(events);
    }

    @AfterEach
    void release() {
        logger.detachAppender(events);
        logger.setLevel(previousLevel);
    }

    private List<ILoggingEvent> at(Level level) {
        return events.list.stream().filter(e -> e.getLevel() == level).toList();
    }

    // W1
    @Test
    void w1_hundredFailuresInOneWindowGiveOneWarn_thenTheSuppressedCountOnTheNextWindow() {
        for (int i = 0; i < 100; i++) {
            outage.failure(new IllegalStateException("down"));
        }
        assertThat(at(Level.WARN)).hasSize(1);
        assertThat(at(Level.WARN).get(0).getFormattedMessage())
                .contains("java.lang.IllegalStateException").contains("0 further failures suppressed");

        clock.advance(WINDOW.plusSeconds(1));
        outage.failure(new IllegalStateException("still down"));
        assertThat(at(Level.WARN)).hasSize(2);
        assertThat(at(Level.WARN).get(1).getFormattedMessage()).contains("99 further failures suppressed");
        assertThat(at(Level.INFO)).isEmpty();
        System.out.println("[LIMITER W1 failures=101 warns=" + at(Level.WARN).size() + " infos=" + at(Level.INFO).size() + "]");
    }

    // W2
    @Test
    void w2_exactlyOneInfoOnRecovery_andANewOutageStartsANewWindow() {
        outage.failure(new RuntimeException("down"));
        outage.failure(new RuntimeException("down"));
        outage.success();
        outage.success();
        outage.success();
        assertThat(at(Level.INFO)).hasSize(1);
        assertThat(at(Level.INFO).get(0).getFormattedMessage()).contains("recovered").contains("1 failures were suppressed");

        outage.failure(new RuntimeException("down again")); // same clock instant: a new outage warns at once
        assertThat(at(Level.WARN)).hasSize(2);
        System.out.println("[LIMITER W2 warns=" + at(Level.WARN).size() + " infos=" + at(Level.INFO).size() + "]");
    }

    // W3
    @Test
    void w3_healthyPathLogsNothing() {
        for (int i = 0; i < 1000; i++) {
            outage.success();
        }
        assertThat(events.list).isEmpty();
        System.out.println("[LIMITER W3 successes=1000 log events=" + events.list.size() + "]");
    }

    // W4: the limiter itself reports through OutageLog (closed port, nothing listens)
    @Test
    void w4_rateLimiterLogsOncePerWindowAgainstAClosedPort() throws Exception {
        int deadPort;
        try (ServerSocket s = new ServerSocket(0)) {
            deadPort = s.getLocalPort();
        }
        LettuceConnectionFactory factory = RedisClientSettings.connectionFactory("localhost", deadPort);
        try {
            StringRedisTemplate template = new StringRedisTemplate(factory);
            template.afterPropertiesSet();
            RateLimiter limiter = new RateLimiter(template, outage);
            for (int i = 0; i < 30; i++) {
                RateLimitDecision d = limiter.tryAcquire("w4", 1, 1.0);
                assertThat(d.allowed()).isTrue();
                assertThat(d.degraded()).isTrue();
            }
            assertThat(at(Level.WARN)).hasSize(1);
            clock.advance(WINDOW.plusSeconds(1));
            limiter.tryAcquire("w4", 1, 1.0);
            assertThat(at(Level.WARN)).hasSize(2);
            assertThat(at(Level.WARN).get(1).getFormattedMessage()).contains("29 further failures suppressed");
            System.out.println("[LIMITER W4 calls=31 warns=" + at(Level.WARN).size() + "]");
        } finally {
            factory.destroy();
        }
    }
}
