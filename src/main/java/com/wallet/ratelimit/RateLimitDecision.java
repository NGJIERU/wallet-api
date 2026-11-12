package com.wallet.ratelimit;

/**
 * @param allowed      the request may proceed
 * @param remaining    whole tokens left after this call (-1 when degraded: unknown)
 * @param retryAfterMs milliseconds until one token exists (0 when allowed)
 * @param degraded     true only when Redis failed and the request was allowed anyway (fail-open)
 */
public record RateLimitDecision(boolean allowed, long remaining, long retryAfterMs, boolean degraded) {

    static RateLimitDecision failedOpen() {
        return new RateLimitDecision(true, -1, 0, true);
    }
}
