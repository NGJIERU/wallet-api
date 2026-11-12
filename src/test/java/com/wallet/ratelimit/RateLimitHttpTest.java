package com.wallet.ratelimit;

import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** H1 to H9: the limiter over HTTP in the limiter-on context (money 5, auth 3, default 10, negligible refill). */
class RateLimitHttpTest extends AbstractRateLimitIntegrationTest {

    private long[] counts() {
        return new long[]{count("transactions"), count("ledger_entries"), count("idempotency_keys"), count("outbox_events")};
    }

    private long count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }

    private void exhaustMoney(String token, long accountId) throws Exception {
        for (int i = 0; i < 5; i++) {
            deposit(token, accountId, key()).andExpect(status().isCreated());
        }
    }

    // H1
    @Test
    void h1_exactlyFiveDepositsThenTooManyRequests_andTheDeniedRequestWritesNothing() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        long[] start = counts();
        exhaustMoney(token, accountId); // exactly 5 allowed: one token per request (no double registration)
        long[] afterFive = counts();
        assertThat(afterFive[1] - start[1]).as("ledger rows for 5 deposits").isEqualTo(5);
        assertThat(afterFive[2] - start[2]).as("idempotency rows for 5 deposits").isEqualTo(5);
        assertThat(afterFive[3] - start[3]).as("outbox rows for 5 deposits").isEqualTo(5);

        MvcResult denied = deposit(token, accountId, key())
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string("X-RateLimit-Remaining", "0"))
                .andExpect(header().exists("Retry-After"))
                .andReturn();
        System.out.println("[LIMITER H1 allowed=5 sixth=" + denied.getResponse().getStatus()
                + " Retry-After=" + denied.getResponse().getHeader("Retry-After") + "]");
        assertThat(Long.parseLong(denied.getResponse().getHeader("Retry-After"))).isGreaterThanOrEqualTo(1);
        assertThat(counts()).as("no ledger, outbox or idempotency rows from the denied request").containsExactly(afterFive);
    }

    // H2
    @Test
    void h2_anotherUserIsNotAffectedByAnExhaustedUser() throws Exception {
        String a = registerAndLogin();
        long accountA = createAccount(a);
        exhaustMoney(a, accountA);
        deposit(a, accountA, key()).andExpect(status().isTooManyRequests());

        String b = registerAndLogin();
        long accountB = createAccount(b);
        deposit(b, accountB, key()).andExpect(status().isCreated());
    }

    // H3
    @Test
    void h3_readsUseAnotherBucketThanMoneyWrites() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        exhaustMoney(token, accountId);
        deposit(token, accountId, key()).andExpect(status().isTooManyRequests());
        get("/accounts", token).andExpect(status().isOk());
    }

    // H4
    @Test
    void h4_loginIsLimitedPerClientIp() throws Exception {
        String registerIp = nextIp();
        String body = credentials("user-" + UUID.randomUUID() + "@example.test");
        authPost("/auth/register", registerIp, body).andExpect(status().isCreated());

        String ipA = nextIp();
        String ipB = nextIp();
        for (int i = 0; i < 3; i++) {
            authPost("/auth/login", ipA, body).andExpect(status().isOk());
        }
        authPost("/auth/login", ipA, body)
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"));
        authPost("/auth/login", ipB, body).andExpect(status().isOk());
    }

    // H5
    @Test
    void h5_healthIsNeverLimited() throws Exception {
        for (int i = 0; i < 15; i++) { // more than every capacity
            mvc.perform(MockMvcRequestBuilders.get("/actuator/health")).andExpect(status().isOk());
        }
    }

    // H6
    @Test
    @SuppressWarnings("unchecked")
    void h6_429BodyHasTheSameFieldsAsAnAdviceBody() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        exhaustMoney(token, accountId);
        MvcResult limited = deposit(token, accountId, key()).andExpect(status().isTooManyRequests()).andReturn();

        String other = registerAndLogin();
        MvcResult advice = get("/accounts/999999999", other).andExpect(status().isNotFound()).andReturn();

        Map<String, Object> limitedBody = mapper.readValue(limited.getResponse().getContentAsString(), Map.class);
        Map<String, Object> adviceBody = mapper.readValue(advice.getResponse().getContentAsString(), Map.class);
        System.out.println("[LIMITER H6 429 fields=" + limitedBody.keySet() + " advice(404) fields=" + adviceBody.keySet()
                + " 429 type=" + limited.getResponse().getContentType() + " advice type=" + advice.getResponse().getContentType() + "]");
        assertThat(limited.getResponse().getContentType()).isEqualTo(advice.getResponse().getContentType());
        assertThat(limitedBody.keySet()).containsExactlyInAnyOrderElementsOf(adviceBody.keySet());
        assertThat(limitedBody.get("status")).isEqualTo(429);
    }

    // H7
    @Test
    void h7_anIdempotentReplayConsumesATokenAndStillReturnsTheStoredResponse() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        String k = key();
        MvcResult first = deposit(token, accountId, k).andExpect(status().isCreated())
                .andExpect(header().string("X-RateLimit-Remaining", "4")).andReturn();
        MvcResult replay = deposit(token, accountId, k).andExpect(status().isCreated())
                .andExpect(header().string("X-RateLimit-Remaining", "3")).andReturn();
        assertThat(mapper.readTree(replay.getResponse().getContentAsString()).get("id"))
                .isEqualTo(mapper.readTree(first.getResponse().getContentAsString()).get("id"));
    }

    // H8
    @Test
    void h8_invalidBearerTokenIsAlways401NeverLimited_documentedLimitation() throws Exception {
        for (int i = 0; i < 12; i++) { // more than the default capacity
            mvc.perform(MockMvcRequestBuilders.get("/accounts").header("Authorization", "Bearer not-a-real-token"))
                    .andExpect(status().isUnauthorized());
        }
        for (int i = 0; i < 8; i++) { // more than the money capacity
            mvc.perform(MockMvcRequestBuilders.post("/accounts/1/deposits")
                            .header("Authorization", "Bearer not-a-real-token")
                            .header("Idempotency-Key", key())
                            .contentType(APPLICATION_JSON).content("{\"amount\":100}"))
                    .andExpect(status().isUnauthorized());
        }
    }

    // H9
    @Test
    void h9_remainingHeaderCountsDown() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        for (int expected = 4; expected >= 0; expected--) {
            MvcResult r = deposit(token, accountId, key()).andExpect(status().isCreated()).andReturn();
            System.out.println("[LIMITER H9 remaining=" + r.getResponse().getHeader("X-RateLimit-Remaining") + "]");
            assertThat(r.getResponse().getHeader("X-RateLimit-Remaining")).isEqualTo(Integer.toString(expected));
        }
    }
}
