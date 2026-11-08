package com.wallet.notification;

import com.wallet.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.UUID;

/**
 * Idempotent handler: ONE database transaction does (1) the processed_events marker, (2) the notification.
 * Both commit together or neither does. A second delivery of the same event hits the marker and does nothing.
 * Never logs the payload.
 */
@Service
public class NotificationHandler {

    private static final Logger log = LoggerFactory.getLogger(NotificationHandler.class);

    public enum Result { CREATED, DUPLICATE, SKIPPED_UNKNOWN_TYPE }

    private record Event(UUID transactionId, long ledgerEntryId, long accountId, long ownerUserId, String direction,
                         String transactionType, long amount, String currency, Instant occurredAt) {
    }

    private static final Set<String> DIRECTIONS = Set.of("DEBIT", "CREDIT");
    private static final Set<String> TYPES = Set.of("DEPOSIT", "WITHDRAWAL", "TRANSFER");

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;
    private final String consumerGroup;

    public NotificationHandler(JdbcClient jdbc, ObjectMapper mapper,
                               @Value("${app.notifications.consumer-group:wallet-notifications}") String consumerGroup) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.consumerGroup = consumerGroup;
    }

    @Transactional
    public Result handle(String eventIdHeader, String eventType, String payload) {
        long eventId = parseEventId(eventIdHeader);
        if (!OutboxWriter.TYPE_TRANSACTION_COMPLETED_V1.equals(eventType)) {
            log.warn("skipping event {} with unknown event type {}", eventId, eventType);
            return Result.SKIPPED_UNKNOWN_TYPE;
        }
        int marked = jdbc.sql("INSERT INTO processed_events (consumer_group, event_id) VALUES (?, ?) ON CONFLICT DO NOTHING")
                .param(consumerGroup).param(eventId).update();
        if (marked == 0) {
            return Result.DUPLICATE;
        }
        Event e = parse(payload);
        jdbc.sql("""
                        INSERT INTO notifications (event_id, user_id, account_id, transaction_id, ledger_entry_id,
                                                   transaction_type, direction, amount, currency, message, occurred_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """)
                .param(eventId).param(e.ownerUserId()).param(e.accountId()).param(e.transactionId())
                .param(e.ledgerEntryId()).param(e.transactionType()).param(e.direction()).param(e.amount())
                .param(e.currency())
                .param(NotificationMessages.render(e.direction(), e.transactionType(), e.amount(), e.currency()))
                .param(OffsetDateTime.ofInstant(e.occurredAt(), ZoneOffset.UTC))
                .update();
        return Result.CREATED;
    }

    private static long parseEventId(String header) {
        if (header == null) {
            throw new PoisonEventException("event-id header is missing");
        }
        try {
            return Long.parseLong(header);
        } catch (NumberFormatException e) {
            throw new PoisonEventException("event-id header is not a number", e);
        }
    }

    private Event parse(String payload) {
        if (payload == null) {
            throw new PoisonEventException("payload is missing");
        }
        JsonNode n;
        try {
            n = mapper.readTree(payload);
        } catch (JacksonException e) {
            throw new PoisonEventException("payload is not valid JSON", e);
        }
        if (n == null || !n.isObject()) {
            throw new PoisonEventException("payload is not a JSON object");
        }
        try {
            UUID txn = UUID.fromString(text(n, "transactionId"));
            Instant occurredAt = Instant.parse(text(n, "occurredAt"));
            String direction = text(n, "direction");
            String type = text(n, "transactionType");
            String currency = text(n, "currency");
            long amount = number(n, "amount");
            if (!DIRECTIONS.contains(direction)) {
                throw new PoisonEventException("direction is not DEBIT or CREDIT");
            }
            if (!TYPES.contains(type)) {
                throw new PoisonEventException("transactionType is not known");
            }
            if (amount <= 0) {
                throw new PoisonEventException("amount is not positive");
            }
            if (!currency.matches("^[A-Z]{3}$")) {
                throw new PoisonEventException("currency is not a 3-letter code");
            }
            return new Event(txn, number(n, "ledgerEntryId"), number(n, "accountId"), number(n, "ownerUserId"),
                    direction, type, amount, currency, occurredAt);
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new PoisonEventException("a payload field has an invalid format", e);
        }
    }

    private static String text(JsonNode n, String field) {
        if (!n.hasNonNull(field)) {
            throw new PoisonEventException("field " + field + " is missing");
        }
        return n.get(field).asString();
    }

    private static long number(JsonNode n, String field) {
        if (!n.hasNonNull(field) || !n.get(field).isIntegralNumber() || !n.get(field).canConvertToLong()) {
            throw new PoisonEventException("field " + field + " is missing or not an integer");
        }
        return n.get(field).asLong();
    }
}
