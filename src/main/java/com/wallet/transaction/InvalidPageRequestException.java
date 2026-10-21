package com.wallet.transaction;

public class InvalidPageRequestException extends RuntimeException {

    public InvalidPageRequestException(String message) {
        super(message);
    }
}