package com.wallet.idempotency;

public record StoredResponse(int code, String body) {
}