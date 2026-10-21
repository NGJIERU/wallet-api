package com.wallet.idempotency;

public class IdempotencyKeyReuseException extends RuntimeException {

    public IdempotencyKeyReuseException() {
        super("Idempotency-Key was already used with a different request");
    }
}