package com.wallet.ratelimit;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

import java.time.Duration;

/**
 * The one place the limiter's Redis client options are defined; tests and the Spring wiring both use it.
 * Values are chosen, not measured. REJECT_COMMANDS makes commands fail at once while the connection is down
 * instead of queueing until the timeout.
 */
public final class RedisClientSettings {

    public static final Duration COMMAND_TIMEOUT = Duration.ofMillis(100);
    public static final Duration CONNECT_TIMEOUT = Duration.ofMillis(500);

    private RedisClientSettings() {
    }

    public static LettuceClientConfiguration clientConfiguration() {
        SocketOptions socket = SocketOptions.builder().connectTimeout(CONNECT_TIMEOUT).build();
        TimeoutOptions timeouts = new TimeoutOptions.Builder().timeoutCommands().fixedTimeout(COMMAND_TIMEOUT).build();
        ClientOptions options = ClientOptions.builder()
                .autoReconnect(true)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .socketOptions(socket)
                .timeoutOptions(timeouts)
                .build();
        return LettuceClientConfiguration.builder()
                .commandTimeout(COMMAND_TIMEOUT)
                .clientOptions(options)
                .build();
    }

    /** Standalone factory, initialised and ready (for tests and plain use; Spring wiring comes in 6.2). */
    public static LettuceConnectionFactory connectionFactory(String host, int port) {
        LettuceConnectionFactory factory =
                new LettuceConnectionFactory(new RedisStandaloneConfiguration(host, port), clientConfiguration());
        factory.afterPropertiesSet();
        return factory;
    }
}
