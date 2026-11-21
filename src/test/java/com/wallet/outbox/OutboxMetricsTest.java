package com.wallet.outbox;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.wallet.AbstractIntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** GA1 to GA3: the outbox gauges with their own registry and a fake clock (no dependence on the shared registry). */
class OutboxMetricsTest extends AbstractIntegrationTest {

    private static final String UNPUBLISHED = "wallet.outbox.unpublished";
    private static final String OLDEST = "wallet.outbox.oldest.age.seconds";

    @Autowired JdbcClient jdbc;
    @Autowired OutboxRelay relay;

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

    private static double value(MeterRegistry reg, String name) {
        return reg.get(name).gauge().value();
    }

    private void insertRows(String aggregateId, int n) {
        for (int i = 0; i < n; i++) {
            jdbc.sql("INSERT INTO outbox_events (aggregate_id, type, payload) VALUES (?, ?, CAST(? AS jsonb))")
                    .param(aggregateId).param("TransactionCompleted.v1").param("{}").update();
        }
    }

    private void deleteRows(String aggregateId) {
        jdbc.sql("DELETE FROM outbox_events WHERE aggregate_id = ?").param(aggregateId).update();
    }

    private long unpublishedInDb() {
        return jdbc.sql("SELECT count(*) FROM outbox_events WHERE published_at IS NULL").query(Long.class).single();
    }

    // GA1
    @Test
    void ga1_gaugeValuesMatchTheDatabaseAndTheAgeIsZeroWhenNothingIsUnpublished() {
        RelaySupport.drain(relay); // the relay is off in tests: start from nothing unpublished
        MutableClock clock = new MutableClock();
        MeterRegistry reg = new SimpleMeterRegistry();
        new OutboxMetrics(jdbc, reg, clock);
        assertThat(value(reg, UNPUBLISHED)).isEqualTo(0.0);
        assertThat(value(reg, OLDEST)).isEqualTo(0.0);

        String agg = "metrics-" + UUID.randomUUID();
        try {
            insertRows(agg, 3);
            clock.advance(Duration.ofSeconds(6));
            assertThat(value(reg, UNPUBLISHED)).isEqualTo((double) unpublishedInDb()).isEqualTo(3.0);
            assertThat(value(reg, OLDEST)).isGreaterThan(0.0);
        } finally {
            deleteRows(agg);
        }
        clock.advance(Duration.ofSeconds(6));
        assertThat(value(reg, UNPUBLISHED)).isEqualTo(0.0);
        assertThat(value(reg, OLDEST)).isEqualTo(0.0);
    }

    // GA2
    @Test
    void ga2_aDatabaseChangeIsNotVisibleBeforeTheRefreshWindowAndIsVisibleAfter() {
        RelaySupport.drain(relay);
        MutableClock clock = new MutableClock();
        MeterRegistry reg = new SimpleMeterRegistry();
        new OutboxMetrics(jdbc, reg, clock);
        assertThat(value(reg, UNPUBLISHED)).isEqualTo(0.0);

        String agg = "metrics-" + UUID.randomUUID();
        try {
            insertRows(agg, 2);
            clock.advance(Duration.ofSeconds(4));
            assertThat(value(reg, UNPUBLISHED)).as("cached: 4 s is inside the 5 s window").isEqualTo(0.0);
            clock.advance(Duration.ofSeconds(2));
            assertThat(value(reg, UNPUBLISHED)).as("refreshed: 6 s is past the window").isEqualTo(2.0);
        } finally {
            deleteRows(agg);
        }
    }

    // GA3
    @Test
    void ga3_aDatabaseErrorGivesNaNWithoutThrowingOrLoggingPerCall() {
        JdbcClient broken = mock(JdbcClient.class);
        when(broken.sql(anyString())).thenThrow(new DataAccessResourceFailureException("db down"));
        MutableClock clock = new MutableClock();
        MeterRegistry reg = new SimpleMeterRegistry();
        new OutboxMetrics(broken, reg, clock);

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ListAppender<ILoggingEvent> events = new ListAppender<>();
        events.start();
        root.addAppender(events);
        try {
            for (int i = 0; i < 3; i++) {
                assertThat(value(reg, UNPUBLISHED)).isNaN();
                assertThat(value(reg, OLDEST)).isNaN();
            }
            verify(broken, times(1)).sql(anyString()); // the error is cached for the window: no per-scrape query
            assertThat(new ArrayList<>(events.list)).noneMatch(e -> e.getLoggerName().startsWith("com.wallet"));
        } finally {
            root.detachAppender(events);
            events.stop();
        }
    }
}
