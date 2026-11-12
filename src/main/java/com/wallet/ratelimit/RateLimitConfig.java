package com.wallet.ratelimit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
import org.springframework.boot.data.redis.autoconfigure.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;

/**
 * The properties are always bound and validated. The limiter and the Redis client settings exist only when
 * app.ratelimit.enabled=true, so a context with the flag off can never throttle anything.
 */
@Configuration
@EnableConfigurationProperties(RateLimitProperties.class)
public class RateLimitConfig {

    @Bean
    @ConditionalOnProperty(name = "app.ratelimit.enabled", havingValue = "true")
    RateLimiter rateLimiter(StringRedisTemplate template, RateLimitProperties props) {
        return new RateLimiter(template, new OutageLog(Clock.systemUTC(), props.logWindow()));
    }

    /** Applies the tested client settings (timeouts, REJECT_COMMANDS) to Boot's auto-configured Lettuce factory. */
    @Bean
    @ConditionalOnProperty(name = "app.ratelimit.enabled", havingValue = "true")
    LettuceClientConfigurationBuilderCustomizer rateLimitLettuceCustomizer() {
        return RedisClientSettings::customize;
    }

    /** Explicit, short-capped reconnect back-off on Boot's ClientResources. */
    @Bean
    @ConditionalOnProperty(name = "app.ratelimit.enabled", havingValue = "true")
    ClientResourcesBuilderCustomizer rateLimitClientResourcesCustomizer() {
        return RedisClientSettings::customizeResources;
    }
}
