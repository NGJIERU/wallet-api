package com.wallet.outbox;

import com.wallet.AbstractIntegrationTest;
import com.wallet.metrics.EventMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** RC1 to RC3: relay counters, each test with its own registry. The relay here uses a fake sender (no Kafka). */
class RelayMetricsTest extends AbstractIntegrationTest {

    private static final String PUBLISHED = "wallet.relay.published";
    private static final String FAILED = "wallet.relay.failed";

    @Autowired JdbcClient jdbc;
    @Autowired OutboxRelay realRelay;

    private static double count(MeterRegistry reg, String name) {
        return reg.get(name).counter().count();
    }

    private List<Long> insertRows(String agg, int n) {
        for (int i = 0; i < n; i++) {
            jdbc.sql("INSERT INTO outbox_events (aggregate_id, type, payload) VALUES (?, ?, CAST(? AS jsonb))")
                    .param(agg).param("TransactionCompleted.v1").param("{}").update();
        }
        return jdbc.sql("SELECT id FROM outbox_events WHERE aggregate_id = ? ORDER BY id").param(agg).query(Long.class).list();
    }

    private void deleteRows(String agg) {
        jdbc.sql("DELETE FROM outbox_events WHERE aggregate_id = ?").param(agg).update();
    }

    // RC1
    @Test
    void rc1_publishedRowsAreCountedAndNothingFailed() {
        RelaySupport.drain(realRelay); // only this test's rows may be unpublished from here on
        MeterRegistry reg = new SimpleMeterRegistry();
        EventMetrics metrics = new EventMetrics(reg);
        assertThat(count(reg, PUBLISHED)).isZero();
        assertThat(count(reg, FAILED)).isZero();
        OutboxRelay relay = new OutboxRelay(jdbc, row -> { }, 100);
        relay.setMetrics(metrics);
        String agg = "relaymetrics-" + UUID.randomUUID();
        try {
            insertRows(agg, 3);
            OutboxRelay.BatchResult r = relay.publishBatch();
            assertThat(r.failure()).isNull();
            assertThat(r.published()).isEqualTo(3);
            assertThat(count(reg, PUBLISHED)).isEqualTo(3.0);
            assertThat(count(reg, FAILED)).isZero();
        } finally {
            deleteRows(agg);
        }
    }

    // RC2
    @Test
    void rc2_aSenderThatFailsOnceCountsOneFailureAndThenTheRecoveryOnce() {
        RelaySupport.drain(realRelay);
        MeterRegistry reg = new SimpleMeterRegistry();
        EventMetrics metrics = new EventMetrics(reg);
        AtomicBoolean fail = new AtomicBoolean(true);
        OutboxRelay relay = new OutboxRelay(jdbc, row -> {
            if (fail.get()) {
                throw new IllegalStateException("injected send failure");
            }
        }, 100);
        relay.setMetrics(metrics);
        String agg = "relaymetrics-" + UUID.randomUUID();
        try {
            List<Long> ids = insertRows(agg, 3);
            OutboxRelay.BatchResult first = relay.publishBatch();
            assertThat(first.failure()).isNotNull();
            assertThat(first.failedRow().id()).isEqualTo(ids.get(0));
            assertThat(count(reg, FAILED)).isEqualTo(1.0);
            assertThat(count(reg, PUBLISHED)).as("the failed row is not counted as published").isZero();

            fail.set(false);
            OutboxRelay.BatchResult second = relay.publishBatch();
            assertThat(second.failure()).isNull();
            assertThat(count(reg, PUBLISHED)).as("recovery counts each row once").isEqualTo(3.0);
            assertThat(count(reg, FAILED)).isEqualTo(1.0);
        } finally {
            deleteRows(agg);
        }
    }

    // RC3: an exception from the SELECT (outside failed()) is counted once, by the scheduler's catch only
    @Test
    void rc3_aDatabaseErrorBeforeTheTryIsCountedExactlyOnceByTheSchedulerCatch() {
        MeterRegistry reg = new SimpleMeterRegistry();
        EventMetrics metrics = new EventMetrics(reg);
        JdbcClient broken = mock(JdbcClient.class);
        when(broken.sql(anyString())).thenThrow(new DataAccessResourceFailureException("db down"));
        OutboxRelay relay = new OutboxRelay(broken, row -> { }, 100);
        relay.setMetrics(metrics);
        // a one-hour back-off: after the first failure the thread sleeps, so a second increment cannot happen
        OutboxRelayScheduler scheduler = new OutboxRelayScheduler(relay, Duration.ofMillis(500),
                Duration.ofHours(1), Duration.ofHours(2), 100);
        scheduler.setMetrics(metrics);
        scheduler.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> count(reg, FAILED) >= 1.0);
        } finally {
            scheduler.stop();
        }
        assertThat(count(reg, FAILED)).isEqualTo(1.0);
        assertThat(count(reg, PUBLISHED)).isZero();
    }
}
