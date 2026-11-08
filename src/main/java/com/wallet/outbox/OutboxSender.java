package com.wallet.outbox;

/** Sends one outbox row and returns only after the broker acknowledged it. Throws on any failure. */
public interface OutboxSender {

    void send(OutboxRelay.Row row) throws Exception;

    /** Called before the next attempt after a failure (for example to re-declare a missing topic). */
    default void prepareRetry() throws Exception {
    }
}
