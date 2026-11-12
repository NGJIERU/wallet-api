package com.wallet.ratelimit;

import com.wallet.AbstractIntegrationTest;
import jakarta.servlet.Filter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/** G2: in the standard test context (shared by the existing tests) the limiter does not exist. */
class RateLimitDisabledByDefaultTest extends AbstractIntegrationTest {

    @Autowired
    ApplicationContext context;

    @Test
    void g2_noLimiterOrFilterBeanWhenFlagIsOff() {
        assertThat(context.getEnvironment().getProperty("app.ratelimit.enabled")).isEqualTo("false");
        assertThat(context.getBeanNamesForType(RateLimiter.class)).isEmpty();
        assertThat(context.getBeansOfType(Filter.class).values())
                .noneMatch(f -> f.getClass().getName().startsWith("com.wallet.ratelimit"));
    }
}
