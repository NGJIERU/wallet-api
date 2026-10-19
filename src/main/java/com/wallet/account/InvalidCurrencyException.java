package com.wallet.account;

public class InvalidCurrencyException extends RuntimeException {

    public InvalidCurrencyException(String code) {
        super("Not a valid ISO 4217 currency code: " + code);
    }
}