package com.wallet.outbox;

/** Sends one outbox row and returns only after the broker acknowledged it. Throws on any failure. */
public interface OutboxSender {

    void send(OutboxRelay.Row row) throws Exception;
}
