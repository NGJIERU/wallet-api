package com.wallet.transaction;

import java.time.Instant;
import java.util.UUID;

public record TransferResponse(
        UUID id,
        TransactionType type,
        String currency,
        long amount,
        Long fromAccountId,
        Long toAccountId,
        long fromBalanceAfter,
        Instant createdAt) {
}