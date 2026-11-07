package com.wallet.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Publishes unpublished outbox rows in id order, one at a time, and marks each row only after the broker
 * acknowledged it. At-least-once: a crash between the ack and the mark gives a duplicate, never a loss.
 * Deliberately NOT transactional (no DB transaction is held across Kafka I/O) and keeps NO last-seen-id cursor
 * (ids can commit out of order across accounts, so a cursor could skip a row forever).
 * The first failure ends the batch, so a failed row is never overtaken by a later row of the same account.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    public record Row(long id, String aggregateId, String type, String payload) {
    }

    /** failure is null when the whole batch went through. */
    public record BatchResult(int selected, int published, Row failedRow, Throwable failure) {
    }

    private final JdbcClient jdbc;
    private final OutboxSender sender;
    private final int batchSize;

    public OutboxRelay(JdbcClient jdbc, OutboxSender sender,
                       @Value("${app.outbox.relay.batch-size:100}") int batchSize) {
        this.jdbc = jdbc;
        this.sender = sender;
        this.batchSize = batchSize;
    }

    public BatchResult publishBatch() {
        List<Row> rows = jdbc.sql("""
                        SELECT id, aggregate_id, type, CAST(payload AS text) AS payload
                        FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT ?
                        """)
                .param(batchSize)
                .query((rs, n) -> new Row(rs.getLong("id"), rs.getString("aggregate_id"),
                        rs.getString("type"), rs.getString("payload")))
                .list();

        int published = 0;
        for (Row row : rows) {
            try {
                sender.send(row);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                return new BatchResult(rows.size(), published, row, e);
            }
            try {
                int updated = jdbc.sql("UPDATE outbox_events SET published_at = now() WHERE id = ? AND published_at IS NULL")
                        .param(row.id()).update();
                if (updated != 1) {
                    log.warn("outbox row {} was already marked published; possible second relay", row.id());
                }
            } catch (RuntimeException e) {
                // the record is on the topic but the row stays unpublished: it will be sent again (a duplicate)
                return new BatchResult(rows.size(), published, row, e);
            }
            published++;
        }
        return new BatchResult(rows.size(), published, null, null);
    }
}
