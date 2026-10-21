package com.wallet.idempotency;

public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException() {
        super("A request with this Idempotency-Key is still being processed");
    }
}