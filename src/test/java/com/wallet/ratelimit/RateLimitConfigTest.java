package com.wallet.ratelimit;

import io.lettuce.core.ClientOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;

/** No full application context and no Redis container: only the wiring is under test. */
class RateLimitConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(RateLimitConfig.class);

    // G1
    @ParameterizedTest(name = "G1 invalid {0} stops start-up")
    @ValueSource(strings = {
            "app.ratelimit.money.capacity=0",
            "app.ratelimit.money.capacity=-1",
            "app.ratelimit.auth.refill-per-second=0",
            "app.ratelimit.default.refill-per-second=-1",
            "app.ratelimit.auth.refill-per-second=NaN",
            "app.ratelimit.default.capacity=0"})
    void g1_invalidPropertiesFailTheContext(String property) {
        runner.withPropertyValues(property).run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void g1_defaultsAndFlagOffMeansNoLimiterBean() {
        runner.run(ctx -> {
            assertThat(ctx).hasNotFailed();
            RateLimitProperties p = ctx.getBean(RateLimitProperties.class);
            assertThat(p.enabled()).isFalse();
            assertThat(p.money().capacity()).isEqualTo(20);
            assertThat(p.money().refillPerSecond()).isEqualTo(5.0);
            assertThat(p.fallback().capacity()).isEqualTo(60);
            assertThat(p.fallback().refillPerSecond()).isEqualTo(20.0);
            assertThat(p.auth().capacity()).isEqualTo(10);
            assertThat(p.auth().refillPerSecond()).isEqualTo(10.0 / 60);
            assertThat(ctx).doesNotHaveBean(RateLimiter.class);
        });
    }

    // G3: Boot's own factory gets the settings that 6.1 tested (no connection is made here)
    @Test
    void g3_enabledAppliesTheSharedClientSettingsToBootsFactory() {
        runner.withConfiguration(AutoConfigurations.of(DataRedisAutoConfiguration.class))
                .withPropertyValues("app.ratelimit.enabled=true")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx).hasSingleBean(RateLimiter.class);
                    LettuceClientConfiguration cfg = ctx.getBean(LettuceConnectionFactory.class).getClientConfiguration();
                    assertThat(cfg.getCommandTimeout()).isEqualTo(RedisClientSettings.COMMAND_TIMEOUT);
                    assertThat(cfg.getClientOptions().orElseThrow().getDisconnectedBehavior())
                            .isEqualTo(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS);
                });
    }
}
