package com.wallet.notification;

/** A record that can never be processed (bad JSON, missing field, bad header). Retrying cannot help. */
public class PoisonEventException extends RuntimeException {

    public PoisonEventException(String message) {
        super(message);
    }

    public PoisonEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
