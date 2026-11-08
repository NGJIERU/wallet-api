package com.wallet.notification;

import java.math.BigDecimal;
import java.util.Currency;

/** Short deterministic message for a notification. No localisation. */
public final class NotificationMessages {

    private NotificationMessages() {
    }

    public static String render(String direction, String transactionType, long amountMinor, String currency) {
        String verb;
        if ("CREDIT".equals(direction)) {
            verb = "Received";
        } else {
            verb = "WITHDRAWAL".equals(transactionType) ? "Withdrew" : "Sent";
        }
        return verb + " " + currency + " " + amount(amountMinor, currency) + " (" + transactionType.toLowerCase() + ")";
    }

    /** Uses the JDK's fraction digits (SGD 2, JPY 0); an unknown code falls back to the raw minor-unit amount. */
    static String amount(long amountMinor, String currency) {
        try {
            int digits = Currency.getInstance(currency).getDefaultFractionDigits();
            if (digits < 0) {
                return Long.toString(amountMinor);
            }
            return BigDecimal.valueOf(amountMinor, digits).toPlainString();
        } catch (IllegalArgumentException e) {
            return Long.toString(amountMinor);
        }
    }
}
