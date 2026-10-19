package com.wallet.account;

public class DuplicateAccountException extends RuntimeException {

    public DuplicateAccountException() {
        super("Account in this currency already exists");
    }
}