package com.wallet.transaction;

import java.time.Instant;
import java.util.UUID;

/** One row of an account's history. Wrapper types on purpose: JPQL constructor expressions match them cleanly. */
public record TransactionHistoryItem(
        Long ledgerEntryId,
        UUID transactionId,
        TransactionType type,
        Direction direction,
        Long amount,
        String currency,
        Instant createdAt) {
}