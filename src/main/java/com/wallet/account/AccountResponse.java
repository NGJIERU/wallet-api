package com.wallet.account;

import java.time.Instant;

public record AccountResponse(Long id, String currency, long balance, Instant createdAt) {

    static AccountResponse from(Account a) {
        return new AccountResponse(a.getId(), a.getCurrency(), a.getBalance(), a.getCreatedAt());
    }
}