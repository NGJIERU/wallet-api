package com.wallet.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * The two outbox gauges (Q-B: is the outbox draining?). One cached read serves both: the number of unpublished rows
 * and the age in seconds of the oldest one (0 when none), computed with the database's now(). Refreshed at most every
 * 5 s, so a metrics scrape cannot hammer Postgres. On a database error both gauges are NaN for that refresh window;
 * nothing is thrown and nothing is logged. The count is O(backlog) during a long outage (documented limitation).
 */
@Component
public class OutboxMetrics {

    static final Duration REFRESH = Duration.ofSeconds(5);

    private static final String SQL = """
            SELECT (SELECT count(*) FROM outbox_events WHERE published_at IS NULL) AS n,
                   COALESCE((SELECT EXTRACT(EPOCH FROM (now() - created_at)) FROM outbox_events
                             WHERE published_at IS NULL ORDER BY id LIMIT 1), 0) AS age
            """;

    private final JdbcClient jdbc;
    private final Clock clock;
    private Instant loadedAt;
    private double unpublishedValue = Double.NaN;
    private double oldestAgeValue = Double.NaN;

    @Autowired
    public OutboxMetrics(JdbcClient jdbc, MeterRegistry registry) {
        this(jdbc, registry, Clock.systemUTC());
    }

    OutboxMetrics(JdbcClient jdbc, MeterRegistry registry, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
        Gauge.builder("wallet.outbox.unpublished", this, OutboxMetrics::readUnpublished).register(registry);
        Gauge.builder("wallet.outbox.oldest.age.seconds", this, OutboxMetrics::readOldestAge)
                .baseUnit("seconds").register(registry);
    }

    private double readUnpublished() {
        refreshIfStale();
        return unpublishedValue;
    }

    private double readOldestAge() {
        refreshIfStale();
        return oldestAgeValue;
    }

    private synchronized void refreshIfStale() {
        Instant now = clock.instant();
        if (loadedAt != null && now.isBefore(loadedAt.plus(REFRESH))) {
            return;
        }
        loadedAt = now; // an error is cached for the window too, so a down database is not queried per scrape
        try {
            double[] r = jdbc.sql(SQL).query((rs, i) -> new double[]{rs.getLong("n"), rs.getDouble("age")}).single();
            unpublishedValue = r[0];
            oldestAgeValue = r[1];
        } catch (RuntimeException e) {
            unpublishedValue = Double.NaN;
            oldestAgeValue = Double.NaN;
        }
    }
}
