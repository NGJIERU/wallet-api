package com.wallet.ratelimit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.redis.autoconfigure.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * The properties are always bound and validated. The limiter and the Redis client settings exist only when
 * app.ratelimit.enabled=true, so a context with the flag off can never throttle anything.
 */
@Configuration
@EnableConfigurationProperties(RateLimitProperties.class)
public class RateLimitConfig {

    @Bean
    @ConditionalOnProperty(name = "app.ratelimit.enabled", havingValue = "true")
    RateLimiter rateLimiter(StringRedisTemplate template) {
        return new RateLimiter(template);
    }

    /** Applies the tested client settings (timeouts, REJECT_COMMANDS) to Boot's auto-configured Lettuce factory. */
    @Bean
    @ConditionalOnProperty(name = "app.ratelimit.enabled", havingValue = "true")
    LettuceClientConfigurationBuilderCustomizer rateLimitLettuceCustomizer() {
        return RedisClientSettings::customize;
    }
}
