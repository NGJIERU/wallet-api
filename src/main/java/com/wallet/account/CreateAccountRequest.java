package com.wallet.account;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CreateAccountRequest(
        @NotBlank @Pattern(regexp = "^[A-Z]{3}$", message = "must be 3 uppercase letters") String currency) {
}