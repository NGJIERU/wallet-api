package com.wallet.notification;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** CT8: message rendering (pure unit tests). */
class NotificationMessagesTest {

    @Test
    void depositCredit() {
        assertThat(NotificationMessages.render("CREDIT", "DEPOSIT", 2500, "SGD")).isEqualTo("Received SGD 25.00 (deposit)");
    }

    @Test
    void withdrawalDebit() {
        assertThat(NotificationMessages.render("DEBIT", "WITHDRAWAL", 500, "SGD")).isEqualTo("Withdrew SGD 5.00 (withdrawal)");
    }

    @Test
    void transferDebit() {
        assertThat(NotificationMessages.render("DEBIT", "TRANSFER", 1000, "SGD")).isEqualTo("Sent SGD 10.00 (transfer)");
    }

    @Test
    void transferCredit() {
        assertThat(NotificationMessages.render("CREDIT", "TRANSFER", 1000, "SGD")).isEqualTo("Received SGD 10.00 (transfer)");
    }

    @Test
    void currencyWithZeroFractionDigitsHasNoDecimalPoint() {
        assertThat(NotificationMessages.render("CREDIT", "DEPOSIT", 2500, "JPY")).isEqualTo("Received JPY 2500 (deposit)");
    }

    @Test
    void smallAmountKeepsLeadingZero() {
        assertThat(NotificationMessages.render("CREDIT", "DEPOSIT", 1, "SGD")).isEqualTo("Received SGD 0.01 (deposit)");
    }

    @Test
    void anUnknownCurrencyCodeFallsBackToTheRawMinorUnitAmount() {
        assertThat(NotificationMessages.render("CREDIT", "DEPOSIT", 2500, "ZZZ")).isEqualTo("Received ZZZ 2500 (deposit)");
    }
}
