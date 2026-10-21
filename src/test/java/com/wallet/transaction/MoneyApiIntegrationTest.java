package com.wallet.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.AbstractIntegrationTest;
import com.wallet.account.AccountRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class MoneyApiIntegrationTest extends AbstractIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired AccountRepository accounts;
    @Autowired PlatformTransactionManager txManager;

    @Test
    void depositReturns201AndTheBalanceShowsOnTheAccount() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);

        deposit(token, accountId, key(), "{\"amount\":1500}")
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.type").value("DEPOSIT"))
                .andExpect(jsonPath("$.amount").value(1500))
                .andExpect(jsonPath("$.balanceAfter").value(1500))
                .andExpect(jsonPath("$.accountId").value(accountId));

        assertBalance(token, accountId, 1500);
    }

    @Test
    void withdrawalWithinTheBalanceReturns201() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        deposit(token, accountId, key(), "{\"amount\":1000}").andExpect(status().isCreated());

        withdraw(token, accountId, key(), "{\"amount\":400}")
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.type").value("WITHDRAWAL"))
                .andExpect(jsonPath("$.balanceAfter").value(600));

        assertBalance(token, accountId, 600);
    }

    @Test
    void withdrawalBeyondTheBalanceReturns422AndChangesNothing() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        deposit(token, accountId, key(), "{\"amount\":100}").andExpect(status().isCreated());

        withdraw(token, accountId, key(), "{\"amount\":101}")
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.detail").value("Insufficient funds"));

        assertBalance(token, accountId, 100);
    }

    @Test
    void sameKeyAndSameBodyReplaysTheFirstResponse() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        String k = key();

        String first = deposit(token, accountId, k, "{\"amount\":500}")
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String second = deposit(token, accountId, k, "{\"amount\":500}")
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        assertThat(mapper.readTree(second).get("id")).isEqualTo(mapper.readTree(first).get("id"));
        assertBalance(token, accountId, 500); // money moved once
    }

    @Test
    void sameKeyWithADifferentAmountReturns422() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        String k = key();
        deposit(token, accountId, k, "{\"amount\":500}").andExpect(status().isCreated());

        deposit(token, accountId, k, "{\"amount\":600}")
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.detail").value(containsString("Idempotency-Key")));

        assertBalance(token, accountId, 500);
    }

    @Test
    void missingOrMalformedKeyReturns400() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);

        deposit(token, accountId, null, "{\"amount\":100}").andExpect(status().isBadRequest());
        deposit(token, accountId, "has space", "{\"amount\":100}").andExpect(status().isBadRequest());
        deposit(token, accountId, "a".repeat(65), "{\"amount\":100}").andExpect(status().isBadRequest());

        assertBalance(token, accountId, 0);
    }

    @Test
    void invalidAmountsReturn400AndTheMaximumIsAccepted() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);

        for (String body : List.of(
                "{\"amount\":0}", "{\"amount\":-5}", "{}", "{\"amount\":null}",
                "{\"amount\":1.5}", "{\"amount\":\"abc\"}", "{\"amount\":10000000001}")) {
            deposit(token, accountId, key(), body).andExpect(status().isBadRequest());
        }
        assertBalance(token, accountId, 0);

        deposit(token, accountId, key(), "{\"amount\":10000000000}").andExpect(status().isCreated());
        assertBalance(token, accountId, 10_000_000_000L);
    }

    @Test
    void anotherUsersAccountReturns404() throws Exception {
        String owner = registerAndLogin();
        String stranger = registerAndLogin();
        long accountId = createAccount(owner);

        deposit(stranger, accountId, key(), "{\"amount\":100}").andExpect(status().isNotFound());
        withdraw(stranger, accountId, key(), "{\"amount\":100}").andExpect(status().isNotFound());

        assertBalance(owner, accountId, 0);
    }

    @Test
    void noTokenReturns401() throws Exception {
        mvc.perform(MockMvcRequestBuilders.post("/accounts/1/deposits")
                        .header("Idempotency-Key", key())
                        .contentType(APPLICATION_JSON)
                        .content("{\"amount\":100}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void aBusyAccountRowTimesOutAs409AndTheSameKeyWorksAfterwards() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        String k = key();

        TransactionTemplate tx = new TransactionTemplate(txManager);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> holder = pool.submit(() -> tx.executeWithoutResult(s -> {
            accounts.lockAllByIdOrdered(List.of(accountId));
            locked.countDown();
            awaitQuietly(release);
        }));
        try {
            assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();
            // waits for the default 2 s lock_timeout, then maps to 409; the key claim rolls back with it
            deposit(token, accountId, k, "{\"amount\":100}")
                    .andExpect(status().isConflict())
                    .andExpect(jsonPath("$.detail").value(containsString("busy")));
        } finally {
            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
            pool.shutdown();
        }

        deposit(token, accountId, k, "{\"amount\":100}").andExpect(status().isCreated());
        assertBalance(token, accountId, 100);
    }

    private ResultActions deposit(String token, long accountId, String key, String body) throws Exception {
        return send("/accounts/" + accountId + "/deposits", token, key, body);
    }

    private ResultActions withdraw(String token, long accountId, String key, String body) throws Exception {
        return send("/accounts/" + accountId + "/withdrawals", token, key, body);
    }

    private ResultActions send(String path, String token, String key, String body) throws Exception {
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders.post(path)
                .header("Authorization", "Bearer " + token)
                .contentType(APPLICATION_JSON)
                .content(body);
        if (key != null) {
            request.header("Idempotency-Key", key);
        }
        return mvc.perform(request);
    }

    private void assertBalance(String token, long accountId, long expected) throws Exception {
        mvc.perform(get("/accounts/" + accountId).header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.balance").value(expected));
    }

    private long createAccount(String token) throws Exception {
        String response = mvc.perform(MockMvcRequestBuilders.post("/accounts")
                        .header("Authorization", "Bearer " + token)
                        .contentType(APPLICATION_JSON)
                        .content("{\"currency\":\"SGD\"}"))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(response).get("id").asLong();
    }

    private String registerAndLogin() throws Exception {
        String body = "{\"email\":\"user-" + UUID.randomUUID() + "@example.test\",\"password\":\"password123\"}";
        mvc.perform(MockMvcRequestBuilders.post("/auth/register").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isCreated());
        String response = mvc.perform(MockMvcRequestBuilders.post("/auth/login").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(response).get("accessToken").asText();
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}