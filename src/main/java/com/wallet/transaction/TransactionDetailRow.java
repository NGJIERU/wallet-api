package com.wallet.transaction;

import java.time.Instant;
import java.util.UUID;

/** Flat row from the detail query: one per ledger entry the caller owns. Public so Hibernate can instantiate it. */
public record TransactionDetailRow(
        UUID id,
        TransactionType type,
        String currency,
        Long amount,
        Instant createdAt,
        Long accountId,
        Direction direction) {
}