package com.wallet.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class TransferApiIntegrationTest extends AbstractIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;

    @Test
    void transferReturns201AndMovesTheMoneyBetweenUsers() throws Exception {
        String alice = registerAndLogin();
        String bob = registerAndLogin();
        long from = createAccount(alice);
        long to = createAccount(bob);
        fund(alice, from, 1_000);

        transfer(alice, key(), from, to, 400)
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.type").value("TRANSFER"))
                .andExpect(jsonPath("$.amount").value(400))
                .andExpect(jsonPath("$.fromAccountId").value(from))
                .andExpect(jsonPath("$.toAccountId").value(to))
                .andExpect(jsonPath("$.fromBalanceAfter").value(600));

        assertBalance(alice, from, 600);
        assertBalance(bob, to, 400);
    }

    @Test
    void insufficientFundsReturns422AndChangesNothing() throws Exception {
        String alice = registerAndLogin();
        String bob = registerAndLogin();
        long from = createAccount(alice);
        long to = createAccount(bob);
        fund(alice, from, 100);

        transfer(alice, key(), from, to, 101)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.detail").value("Insufficient funds"));

        assertBalance(alice, from, 100);
        assertBalance(bob, to, 0);
    }

    @Test
    void sameKeyAndSameBodyReplaysTheFirstResponse() throws Exception {
        String alice = registerAndLogin();
        String bob = registerAndLogin();
        long from = createAccount(alice);
        long to = createAccount(bob);
        fund(alice, from, 1_000);
        String k = key();

        String first = transfer(alice, k, from, to, 300)
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        String second = transfer(alice, k, from, to, 300)
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();

        assertThat(mapper.readTree(second).get("id")).isEqualTo(mapper.readTree(first).get("id"));
        assertBalance(alice, from, 700); // money moved once
        assertBalance(bob, to, 300);
    }

    @Test
    void sameKeyWithADifferentBodyReturns422() throws Exception {
        String alice = registerAndLogin();
        String bob = registerAndLogin();
        long from = createAccount(alice);
        long to = createAccount(bob);
        fund(alice, from, 1_000);
        String k = key();
        transfer(alice, k, from, to, 300).andExpect(status().isCreated());

        transfer(alice, k, from, to, 301)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.detail").value(containsString("Idempotency-Key")));

        assertBalance(alice, from, 700);
        assertBalance(bob, to, 300);
    }

    @Test
    void aMissingDestinationReturns404WithTheAccountIdInTheMessage() throws Exception {
        String alice = registerAndLogin();
        long from = createAccount(alice);
        fund(alice, from, 1_000);

        transfer(alice, key(), from, 999_999_999_999L, 100)
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Destination account 999999999999 not found"));

        assertBalance(alice, from, 1_000);
    }

    @Test
    void sendingFromSomeoneElsesAccountReturns404() throws Exception {
        String alice = registerAndLogin();
        String mallory = registerAndLogin();
        long aliceAccount = createAccount(alice);
        long malloryAccount = createAccount(mallory);
        fund(alice, aliceAccount, 1_000);

        transfer(mallory, key(), aliceAccount, malloryAccount, 500)
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Account not found"));

        assertBalance(alice, aliceAccount, 1_000);
        assertBalance(mallory, malloryAccount, 0);
    }

    @Test
    void sameAccountReturns400() throws Exception {
        String alice = registerAndLogin();
        long from = createAccount(alice);
        fund(alice, from, 1_000);

        transfer(alice, key(), from, from, 100)
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Cannot transfer to the same account"));

        assertBalance(alice, from, 1_000);
    }

    @Test
    void missingKeyReturns400AndNoTokenReturns401() throws Exception {
        String alice = registerAndLogin();
        String bob = registerAndLogin();
        long from = createAccount(alice);
        long to = createAccount(bob);

        transfer(alice, null, from, to, 100).andExpect(status().isBadRequest());

        mvc.perform(MockMvcRequestBuilders.post("/transfers")
                        .header("Idempotency-Key", key())
                        .contentType(APPLICATION_JSON)
                        .content(body(from, to, 100)))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void invalidBodiesReturn400() throws Exception {
        String alice = registerAndLogin();
        String bob = registerAndLogin();
        long from = createAccount(alice);
        long to = createAccount(bob);
        fund(alice, from, 1_000);

        for (String body : List.of(
                "{}",
                "{\"fromAccountId\":" + from + ",\"toAccountId\":" + to + "}",
                "{\"fromAccountId\":" + from + ",\"toAccountId\":" + to + ",\"amount\":0}",
                "{\"fromAccountId\":" + from + ",\"toAccountId\":" + to + ",\"amount\":-5}",
                "{\"fromAccountId\":" + from + ",\"toAccountId\":" + to + ",\"amount\":1.5}",
                "{\"fromAccountId\":" + from + ",\"toAccountId\":" + to + ",\"amount\":10000000001}",
                "{\"fromAccountId\":-1,\"toAccountId\":" + to + ",\"amount\":100}",
                "{\"fromAccountId\":" + from + ",\"toAccountId\":null,\"amount\":100}")) {
            send(alice, key(), body).andExpect(status().isBadRequest());
        }

        assertBalance(alice, from, 1_000);
        assertBalance(bob, to, 0);
    }

    private ResultActions transfer(String token, String key, long from, long to, long amount) throws Exception {
        return send(token, key, body(from, to, amount));
    }

    private static String body(long from, long to, long amount) {
        return "{\"fromAccountId\":" + from + ",\"toAccountId\":" + to + ",\"amount\":" + amount + "}";
    }

    private ResultActions send(String token, String key, String body) throws Exception {
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders.post("/transfers")
                .header("Authorization", "Bearer " + token)
                .contentType(APPLICATION_JSON)
                .content(body);
        if (key != null) {
            request.header("Idempotency-Key", key);
        }
        return mvc.perform(request);
    }

    private void fund(String token, long accountId, long amount) throws Exception {
        mvc.perform(MockMvcRequestBuilders.post("/accounts/" + accountId + "/deposits")
                        .header("Authorization", "Bearer " + token)
                        .header("Idempotency-Key", key())
                        .contentType(APPLICATION_JSON)
                        .content("{\"amount\":%d}".formatted(amount)))
                .andExpect(status().isCreated());
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
}