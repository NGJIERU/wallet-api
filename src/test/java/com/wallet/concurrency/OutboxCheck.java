package com.wallet.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Scoped outbox-versus-ledger checks. Deliberately NOT a global hook (see brief section 6.4). */
final class OutboxCheck {

    private static final Logger log = LoggerFactory.getLogger(OutboxCheck.class);

    private OutboxCheck() {
    }

    private static List<String> agg(List<Long> ids) {
        return ids.stream().map(String::valueOf).toList();
    }

    static long events(JdbcClient jdbc, List<Long> ids) {
        return jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id IN (:agg)")
                .param("agg", agg(ids)).query(Long.class).single();
    }

    static long eventsWhere(JdbcClient jdbc, List<Long> ids, String payloadField, String value) {
        return jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id IN (:agg) AND payload->>'"
                        + payloadField + "' = :v")
                .param("agg", agg(ids)).param("v", value).query(Long.class).single();
    }

    static long ledgerWithoutEvent(JdbcClient jdbc, List<Long> ids) {
        return jdbc.sql("""
                        SELECT COUNT(*) FROM ledger_entries l
                        WHERE l.account_id IN (:ids)
                          AND NOT EXISTS (SELECT 1 FROM outbox_events o
                                          WHERE CAST(o.payload->>'ledgerEntryId' AS bigint) = l.id)
                        """)
                .param("ids", ids).query(Long.class).single();
    }

    static long orphanEvents(JdbcClient jdbc, List<Long> ids) {
        return jdbc.sql("""
                        SELECT COUNT(*) FROM outbox_events o
                        WHERE o.aggregate_id IN (:agg)
                          AND NOT EXISTS (SELECT 1 FROM ledger_entries l
                                          WHERE l.id = CAST(o.payload->>'ledgerEntryId' AS bigint)
                                            AND l.account_id IN (:ids))
                        """)
                .param("agg", agg(ids)).param("ids", ids).query(Long.class).single();
    }

    static long duplicateEvents(JdbcClient jdbc, List<Long> ids) {
        return jdbc.sql("""
                        SELECT COUNT(*) FROM (
                            SELECT payload->>'ledgerEntryId' FROM outbox_events
                            WHERE aggregate_id IN (:agg)
                            GROUP BY payload->>'ledgerEntryId' HAVING COUNT(*) > 1) d
                        """)
                .param("agg", agg(ids)).query(Long.class).single();
    }

    static long aggregateMismatches(JdbcClient jdbc, List<Long> ids) {
        return jdbc.sql("""
                        SELECT COUNT(*) FROM outbox_events o
                        JOIN ledger_entries l ON l.id = CAST(o.payload->>'ledgerEntryId' AS bigint)
                        WHERE l.account_id IN (:ids) AND o.aggregate_id <> CAST(l.account_id AS text)
                        """)
                .param("ids", ids).query(Long.class).single();
    }

    /** Per account, outbox ids in order must carry strictly increasing ledger entry ids. */
    static long orderViolations(JdbcClient jdbc, List<Long> ids) {
        return jdbc.sql("""
                        SELECT COUNT(*) FROM (
                            SELECT l.id AS lid,
                                   LAG(l.id) OVER (PARTITION BY l.account_id ORDER BY o.id) AS prev
                            FROM outbox_events o
                            JOIN ledger_entries l ON l.id = CAST(o.payload->>'ledgerEntryId' AS bigint)
                            WHERE l.account_id IN (:ids)) x
                        WHERE prev IS NOT NULL AND lid <= prev
                        """)
                .param("ids", ids).query(Long.class).single();
    }

    static void assertOneToOne(JdbcClient jdbc, String label, List<Long> ids, long expectedLedgerRows) {
        long ledger = jdbc.sql("SELECT COUNT(*) FROM ledger_entries WHERE account_id IN (:ids)")
                .param("ids", ids).query(Long.class).single();
        long events = events(jdbc, ids);
        long missing = ledgerWithoutEvent(jdbc, ids);
        long orphans = orphanEvents(jdbc, ids);
        long duplicates = duplicateEvents(jdbc, ids);
        long mismatches = aggregateMismatches(jdbc, ids);
        long order = orderViolations(jdbc, ids);
        log.info("[OUTBOX {}] ledger={} events={} ledgerWithoutEvent={} orphanEvents={} duplicates={} "
                        + "aggregateMismatches={} perAccountOrderViolations={}",
                label, ledger, events, missing, orphans, duplicates, mismatches, order);

        assertThat(ledger).as("ledger rows").isEqualTo(expectedLedgerRows);
        assertThat(events).as("outbox events").isEqualTo(expectedLedgerRows);
        assertThat(missing).as("ledger entries with no event").isZero();
        assertThat(orphans).as("events with no ledger entry").isZero();
        assertThat(duplicates).as("ledger entries with more than one event").isZero();
        assertThat(mismatches).as("events whose aggregate_id differs from the ledger account").isZero();
        assertThat(order).as("per-account order violations (outbox id order vs ledger id order)").isZero();
    }
}
