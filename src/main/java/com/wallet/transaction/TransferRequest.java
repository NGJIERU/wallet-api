package com.wallet.transaction;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

// Long (not long) so a missing field becomes null and fails @NotNull instead of defaulting to 0.
public record TransferRequest(
        @NotNull @Positive Long fromAccountId,
        @NotNull @Positive Long toAccountId,
        @NotNull @Positive @Max(10_000_000_000L) Long amount) {
}