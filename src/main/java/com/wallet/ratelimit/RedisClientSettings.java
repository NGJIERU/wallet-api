package com.wallet.ratelimit;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.Delay;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The one place the limiter's Redis client options are defined; the tests and Boot's auto-configured
 * factory (through RateLimitConfig) both use it. Values are chosen, not measured. REJECT_COMMANDS makes
 * commands fail at once while the connection is down instead of queueing until the timeout.
 */
public final class RedisClientSettings {

    public static final Duration COMMAND_TIMEOUT = Duration.ofMillis(100);
    public static final Duration CONNECT_TIMEOUT = Duration.ofMillis(500);
    /** Reconnect back-off: exponential (base 2), never below 100 ms, capped at 2 s. Chosen, not measured. */
    public static final Duration RECONNECT_MIN_DELAY = Duration.ofMillis(100);
    public static final Duration RECONNECT_MAX_DELAY = Duration.ofSeconds(2);

    private RedisClientSettings() {
    }

    public static ClientOptions clientOptions() {
        SocketOptions socket = SocketOptions.builder().connectTimeout(CONNECT_TIMEOUT).build();
        TimeoutOptions timeouts = new TimeoutOptions.Builder().timeoutCommands().fixedTimeout(COMMAND_TIMEOUT).build();
        return ClientOptions.builder()
                .autoReconnect(true)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .socketOptions(socket)
                .timeoutOptions(timeouts)
                .build();
    }

    /** Lettuce's default reconnect cap is 30 s; after a Redis restart that would delay recovery by up to that long. */
    public static void customizeResources(ClientResources.Builder builder) {
        builder.reconnectDelay(Delay.exponential(RECONNECT_MIN_DELAY, RECONNECT_MAX_DELAY, 2, TimeUnit.MILLISECONDS));
    }

    /** Applies the shared settings to any Lettuce client configuration builder (Boot's or our own). */
    public static void customize(LettuceClientConfiguration.LettuceClientConfigurationBuilder builder) {
        builder.commandTimeout(COMMAND_TIMEOUT).clientOptions(clientOptions());
    }

    public static LettuceClientConfiguration clientConfiguration() {
        LettuceClientConfiguration.LettuceClientConfigurationBuilder builder = LettuceClientConfiguration.builder();
        customize(builder);
        return builder.build();
    }

    /** Standalone factory, initialised and ready (for tests and plain use). */
    public static LettuceConnectionFactory connectionFactory(String host, int port) {
        LettuceConnectionFactory factory =
                new LettuceConnectionFactory(new RedisStandaloneConfiguration(host, port), clientConfiguration());
        factory.afterPropertiesSet();
        return factory;
    }
}
