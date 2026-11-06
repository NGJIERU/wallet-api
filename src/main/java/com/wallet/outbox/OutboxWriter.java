package com.wallet.outbox;

import com.wallet.account.Account;
import com.wallet.transaction.LedgerEntry;
import com.wallet.transaction.Transaction;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Writes one outbox row per ledger entry, INSIDE the money transaction (MANDATORY).
 * Fails closed: any problem here throws and rolls the whole money movement back.
 */
@Service
public class OutboxWriter {

    public static final String TYPE_TRANSACTION_COMPLETED_V1 = "TransactionCompleted.v1";

    /** Payload v1. Strings for ids and timestamps so the wire format does not depend on mapper defaults. */
    public record TransactionCompletedV1(
            String transactionId,
            long ledgerEntryId,
            long accountId,
            long ownerUserId,
            String direction,
            String transactionType,
            long amount,
            String currency,
            String occurredAt) {
    }

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    public OutboxWriter(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void write(Account account, Transaction txn, LedgerEntry entry) {
        if (entry.getId() == null) {
            throw new IllegalStateException("ledger entry has no id yet; cannot write its outbox event");
        }
        if (!account.getId().equals(entry.getAccountId())) {
            throw new IllegalStateException("account does not match the ledger entry");
        }

        TransactionCompletedV1 payload = new TransactionCompletedV1(
                txn.getId().toString(),
                entry.getId(),
                account.getId(),
                account.getUserId(),
                entry.getDirection().name(),
                txn.getType().name(),
                entry.getAmount(),
                txn.getCurrency(),
                txn.getCreatedAt().toString());

        String json;
        try {
            json = mapper.writeValueAsString(payload);
        } catch (JacksonException e) {
            throw new IllegalStateException("could not serialize the outbox payload", e);
        }

        int inserted = jdbc.sql("""
                        INSERT INTO outbox_events (aggregate_id, type, payload)
                        VALUES (?, ?, CAST(? AS jsonb))
                        """)
                .param(String.valueOf(account.getId()))
                .param(TYPE_TRANSACTION_COMPLETED_V1)
                .param(json)
                .update();
        if (inserted != 1) {
            throw new IllegalStateException("outbox insert affected " + inserted + " rows");
        }
    }
}
