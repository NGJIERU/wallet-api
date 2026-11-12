package com.wallet.ratelimit;

import com.wallet.AbstractIntegrationTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The ONE limiter-on Spring context (7th). Every class that extends this shares it (same property set), so 6.3
 * reuses it. Tiny limits with negligible refill make allow/deny deterministic: money 5, auth 3, default 10.
 */
@TestPropertySource(properties = {
        "app.ratelimit.enabled=true",
        "app.ratelimit.money.capacity=5", "app.ratelimit.money.refill-per-second=0.0001",
        "app.ratelimit.auth.capacity=3", "app.ratelimit.auth.refill-per-second=0.0001",
        "app.ratelimit.default.capacity=10", "app.ratelimit.default.refill-per-second=0.0001"})
public abstract class AbstractRateLimitIntegrationTest extends AbstractIntegrationTest {

    private static final AtomicInteger IP_COUNTER = new AtomicInteger(1);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", LimiterRedis::host);
        registry.add("spring.data.redis.port", LimiterRedis::port);
    }

    @Autowired protected MockMvc mvc;
    @Autowired protected ObjectMapper mapper;
    @Autowired protected JdbcClient jdbc;

    /** A fresh fake client IP, so a test never shares the 3-token auth bucket with another test. */
    protected static String nextIp() {
        int n = IP_COUNTER.getAndIncrement();
        return "10." + ((n >> 16) & 255) + "." + ((n >> 8) & 255) + "." + (n & 255);
    }

    protected static String key() {
        return UUID.randomUUID().toString();
    }

    protected ResultActions authPost(String path, String ip, String body) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.post(path)
                .with(r -> {
                    r.setRemoteAddr(ip);
                    return r;
                })
                .contentType(APPLICATION_JSON)
                .content(body));
    }

    protected static String credentials(String email) {
        return "{\"email\":\"" + email + "\",\"password\":\"password123\"}";
    }

    /** Registers and logs in from one fresh IP (2 of that IP's 3 auth tokens). */
    protected String registerAndLogin() throws Exception {
        String ip = nextIp();
        String body = credentials("user-" + UUID.randomUUID() + "@example.test");
        authPost("/auth/register", ip, body).andExpect(status().isCreated());
        String response = authPost("/auth/login", ip, body).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(response).get("accessToken").asString();
    }

    protected long createAccount(String token) throws Exception {
        String response = mvc.perform(MockMvcRequestBuilders.post("/accounts")
                        .header("Authorization", "Bearer " + token)
                        .contentType(APPLICATION_JSON)
                        .content("{\"currency\":\"SGD\"}"))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(response).get("id").asLong();
    }

    protected ResultActions deposit(String token, long accountId, String idempotencyKey) throws Exception {
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders.post("/accounts/" + accountId + "/deposits")
                .header("Authorization", "Bearer " + token)
                .header("Idempotency-Key", idempotencyKey)
                .contentType(APPLICATION_JSON)
                .content("{\"amount\":100}");
        return mvc.perform(request);
    }

    protected ResultActions get(String path, String token) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.get(path).header("Authorization", "Bearer " + token));
    }
}
