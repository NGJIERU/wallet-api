package com.wallet.auth;

public record TokenResponse(String accessToken, String tokenType, long expiresInSeconds) {
}