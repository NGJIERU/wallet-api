package com.wallet.transaction;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** entries holds only the caller's own side(s) of the transaction. */
public record TransactionDetailResponse(
        UUID id,
        TransactionType type,
        String currency,
        long amount,
        Instant createdAt,
        List<Entry> entries) {

    public record Entry(Long accountId, Direction direction) {
    }
}