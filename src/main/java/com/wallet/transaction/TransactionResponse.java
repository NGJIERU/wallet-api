package com.wallet.transaction;

import java.time.Instant;
import java.util.UUID;

public record TransactionResponse(
        UUID id,
        TransactionType type,
        String currency,
        long amount,
        Long accountId,
        long balanceAfter,
        Instant createdAt) {
}