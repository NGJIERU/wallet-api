package com.wallet.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

/**
 * Token bucket on Redis (see scripts/token_bucket.lua). Fail-open: any Redis error or timeout allows the
 * request and marks the decision degraded. No retry. Time is Redis's own clock, so there is no clock here.
 * Not a Spring bean yet.
 */
public class RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> SCRIPT = buildScript();

    private final StringRedisTemplate template;

    public RateLimiter(StringRedisTemplate template) {
        this.template = template;
    }

    @SuppressWarnings("rawtypes")
    private static RedisScript<List> buildScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("scripts/token_bucket.lua"));
        script.setResultType(List.class);
        return script;
    }

    public RateLimitDecision tryAcquire(String key, long capacity, double ratePerSecond) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (!Double.isFinite(ratePerSecond) || ratePerSecond <= 0) {
            throw new IllegalArgumentException("ratePerSecond must be finite and > 0");
        }
        try {
            List<?> r = template.execute(SCRIPT, List.of(key), Long.toString(capacity), Double.toString(ratePerSecond));
            if (r == null || r.size() != 3) {
                throw new IllegalStateException("unexpected script reply: " + r);
            }
            return new RateLimitDecision(((Number) r.get(0)).longValue() == 1,
                    ((Number) r.get(1)).longValue(), ((Number) r.get(2)).longValue(), false);
        } catch (RuntimeException e) {
            log.warn("Rate limiter degraded, request allowed (fail-open): {}", e.toString());
            return RateLimitDecision.failedOpen();
        }
    }
}
