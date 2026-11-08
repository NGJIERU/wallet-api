package com.wallet.notification;

import java.util.UUID;

public final class NotificationTestData {

    /** Microsecond precision, so it round-trips through timestamptz exactly. */
    public static final String OCCURRED_AT = "2026-11-09T01:02:03.456789Z";

    private NotificationTestData() {
    }

    public static String payload(UUID txn, long ledgerEntryId, long accountId, long ownerUserId,
                                 String direction, String type, long amount, String currency) {
        return ("{\"transactionId\":\"%s\",\"ledgerEntryId\":%d,\"accountId\":%d,\"ownerUserId\":%d,"
                + "\"direction\":\"%s\",\"transactionType\":\"%s\",\"amount\":%d,\"currency\":\"%s\","
                + "\"occurredAt\":\"%s\"}")
                .formatted(txn, ledgerEntryId, accountId, ownerUserId, direction, type, amount, currency, OCCURRED_AT);
    }
}
