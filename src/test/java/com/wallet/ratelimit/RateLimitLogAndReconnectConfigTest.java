package com.wallet.ratelimit;

import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.Delay;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** No full context and no Redis: only the new log-window property and the reconnect cap are under test. */
class RateLimitLogAndReconnectConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(RateLimitConfig.class);

    @ParameterizedTest(name = "invalid {0} stops start-up")
    @ValueSource(strings = {"app.ratelimit.log-window=0s", "app.ratelimit.log-window=-1s"})
    void invalidLogWindowFailsTheContext(String property) {
        runner.withPropertyValues(property).run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void logWindowDefaultsToThirtySeconds() {
        runner.run(ctx -> assertThat(ctx.getBean(RateLimitProperties.class).logWindow()).isEqualTo(Duration.ofSeconds(30)));
    }

    @Test
    void reconnectBackoffIsCappedAtTwoSecondsWhenEnabled() {
        runner.withConfiguration(AutoConfigurations.of(DataRedisAutoConfiguration.class))
                .withPropertyValues("app.ratelimit.enabled=true")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    Delay delay = ctx.getBean(ClientResources.class).reconnectDelay();
                    Duration first = delay.createDelay(1);
                    Duration late = delay.createDelay(50);
                    System.out.println("[LIMITER reconnect delay attempt1=" + first.toMillis() + "ms attempt50=" + late.toMillis() + "ms]");
                    assertThat(first).isGreaterThanOrEqualTo(RedisClientSettings.RECONNECT_MIN_DELAY);
                    assertThat(late).isLessThanOrEqualTo(RedisClientSettings.RECONNECT_MAX_DELAY);
                });
    }
}
