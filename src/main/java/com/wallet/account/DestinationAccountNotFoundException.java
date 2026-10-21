package com.wallet.account;

public class DestinationAccountNotFoundException extends RuntimeException {

    public DestinationAccountNotFoundException(long accountId) {
        super("Destination account " + accountId + " not found");
    }
}