package com.wallet.account;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AccountBalanceTest {

    @Test
    void creditAndDebitMoveTheBalance() {
        Account a = new Account(1L, "SGD");
        a.credit(1_000);
        a.debit(400);
        assertThat(a.getBalance()).isEqualTo(600);
    }

    @Test
    void debitingExactlyTheBalanceIsAllowed() {
        Account a = new Account(1L, "SGD");
        a.credit(100);
        a.debit(100);
        assertThat(a.getBalance()).isZero();
    }

    @Test
    void debitingMoreThanTheBalanceThrowsAndChangesNothing() {
        Account a = new Account(1L, "SGD");
        a.credit(100);
        assertThatThrownBy(() -> a.debit(101)).isInstanceOf(InsufficientFundsException.class);
        assertThat(a.getBalance()).isEqualTo(100);
    }

    @Test
    void nonPositiveAmountsAreRejected() {
        Account a = new Account(1L, "SGD");
        assertThatThrownBy(() -> a.credit(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> a.debit(-5)).isInstanceOf(IllegalArgumentException.class);
    }
}