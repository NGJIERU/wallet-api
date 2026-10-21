package com.wallet.transaction;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

// Long (not long) so a missing field becomes null and fails @NotNull instead of defaulting to 0.
// Max is 10,000,000,000 minor units (100 million major units): a sane ceiling, not a business rule.
public record MoneyRequest(@NotNull @Positive @Max(10_000_000_000L) Long amount) {
}