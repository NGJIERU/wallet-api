package com.wallet.ratelimit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Name;

import java.time.Duration;

/**
 * app.ratelimit.*: a bad value stops start-up (even when the limiter is off), so a typo cannot silently
 * disable limiting. Defaults are written here, not tuned: money 20 at 5/s, default 60 at 20/s,
 * auth 10 at 10 per minute (per client IP). The property key for "fallback" is "default".
 */
@ConfigurationProperties(prefix = "app.ratelimit")
public record RateLimitProperties(boolean enabled, Scope auth, Scope money, @Name("default") Scope fallback,
                                  Duration logWindow) {

    /** Raw bound values; after construction of RateLimitProperties both fields are non-null and valid. */
    public record Scope(Long capacity, Double refillPerSecond) {
    }

    public RateLimitProperties {
        logWindow = logWindow == null ? Duration.ofSeconds(30) : logWindow; // one outage WARN per window
        if (logWindow.isZero() || logWindow.isNegative()) {
            throw new IllegalArgumentException("app.ratelimit.log-window must be > 0 but was " + logWindow);
        }
        auth = resolve("auth", auth, 10, 10.0 / 60);
        money = resolve("money", money, 20, 5.0);
        fallback = resolve("default", fallback, 60, 20.0);
    }

    private static Scope resolve(String name, Scope raw, long defaultCapacity, double defaultRate) {
        long capacity = raw == null || raw.capacity() == null ? defaultCapacity : raw.capacity();
        double rate = raw == null || raw.refillPerSecond() == null ? defaultRate : raw.refillPerSecond();
        if (capacity <= 0) {
            throw new IllegalArgumentException("app.ratelimit." + name + ".capacity must be > 0 but was " + capacity);
        }
        if (!Double.isFinite(rate) || rate <= 0) {
            throw new IllegalArgumentException(
                    "app.ratelimit." + name + ".refill-per-second must be finite and > 0 but was " + rate);
        }
        return new Scope(capacity, rate);
    }
}
