package com.wallet.common;

/** Thrown when a transaction kept losing a deadlock or serialization race and the retries ran out. */
public class ContentionException extends RuntimeException {

    public ContentionException(Throwable cause) {
        super("The accounts are busy, retry the request", cause);
    }
}