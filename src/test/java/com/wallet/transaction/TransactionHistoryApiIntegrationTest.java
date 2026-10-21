package com.wallet.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class TransactionHistoryApiIntegrationTest extends AbstractIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcClient jdbc;

    @Test
    void historyIsNewestFirstAndTheCursorWalksToTheEnd() throws Exception {
        String token = registerAndLogin();
        long account = createAccount(token);
        for (int i = 1; i <= 5; i++) {
            deposit(token, account, i * 100L);
        }

        JsonNode page1 = history(token, account, "limit=2");
        assertThat(amounts(page1)).containsExactly(500L, 400L);
        assertThat(page1.get("nextCursor").isNull()).isFalse();

        JsonNode page2 = history(token, account, "limit=2&cursor=" + page1.get("nextCursor").asLong());
        assertThat(amounts(page2)).containsExactly(300L, 200L);

        JsonNode page3 = history(token, account, "limit=2&cursor=" + page2.get("nextCursor").asLong());
        assertThat(amounts(page3)).containsExactly(100L);
        assertThat(page3.get("nextCursor").isNull()).isTrue();

        assertReconciled(jdbc, account);
    }

    @Test
    void aLastPageThatIsExactlyFullHasNoNextCursor() throws Exception {
        String token = registerAndLogin();
        long account = createAccount(token);
        for (int i = 1; i <= 4; i++) {
            deposit(token, account, i * 100L);
        }

        JsonNode page2 = history(token, account,
                "limit=2&cursor=" + history(token, account, "limit=2").get("nextCursor").asLong());

        assertThat(amounts(page2)).containsExactly(200L, 100L);
        assertThat(page2.get("nextCursor").isNull()).isTrue();
    }

    @Test
    void aNewAccountHasAnEmptyHistory() throws Exception {
        String token = registerAndLogin();
        long account = createAccount(token);

        JsonNode page = history(token, account, "limit=20");

        assertThat(page.get("items")).isEmpty();
        assertThat(page.get("nextCursor").isNull()).isTrue();
    }

    @Test
    void aTransferIsADebitForTheSenderAndACreditForTheReceiver() throws Exception {
        String alice = registerAndLogin();
        String bob = registerAndLogin();
        long from = createAccount(alice);
        long to = createAccount(bob);
        deposit(alice, from, 1_000);
        transfer(alice, from, to, 400);

        JsonNode sent = history(alice, from, "limit=20").get("items").get(0);
        assertThat(sent.get("type").asText()).isEqualTo("TRANSFER");
        assertThat(sent.get("direction").asText()).isEqualTo("DEBIT");
        assertThat(sent.get("amount").asLong()).isEqualTo(400);

        JsonNode received = history(bob, to, "limit=20");
        assertThat(received.get("items")).hasSize(1);
        assertThat(received.get("items").get(0).get("direction").asText()).isEqualTo("CREDIT");

        assertReconciled(jdbc, from, to);
    }

    @Test
    void anotherUsersHistoryReturns404AndNoTokenReturns401() throws Exception {
        String owner = registerAndLogin();
        String stranger = registerAndLogin();
        long account = createAccount(owner);

        mvc.perform(get("/accounts/" + account + "/transactions").header("Authorization", "Bearer " + stranger))
                .andExpect(status().isNotFound());
        mvc.perform(get("/accounts/" + account + "/transactions"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void invalidPagingParametersReturn400() throws Exception {
        String token = registerAndLogin();
        long account = createAccount(token);

        for (String query : List.of("limit=0", "limit=101", "limit=abc", "cursor=0", "cursor=abc")) {
            mvc.perform(get("/accounts/" + account + "/transactions?" + query)
                            .header("Authorization", "Bearer " + token))
                    .andExpect(status().isBadRequest());
        }
    }

    @Test
    void aParticipantSeesTheirOwnSideAndAStrangerGets404() throws Exception {
        String alice = registerAndLogin();
        String bob = registerAndLogin();
        String mallory = registerAndLogin();
        long from = createAccount(alice);
        long to = createAccount(bob);
        deposit(alice, from, 1_000);
        String id = transfer(alice, from, to, 400);

        mvc.perform(get("/transactions/" + id).header("Authorization", "Bearer " + alice))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.type").value("TRANSFER"))
                .andExpect(jsonPath("$.amount").value(400))
                .andExpect(jsonPath("$.entries.length()").value(1))
                .andExpect(jsonPath("$.entries[0].accountId").value(from))
                .andExpect(jsonPath("$.entries[0].direction").value("DEBIT"));

        mvc.perform(get("/transactions/" + id).header("Authorization", "Bearer " + bob))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.entries.length()").value(1))
                .andExpect(jsonPath("$.entries[0].accountId").value(to))
                .andExpect(jsonPath("$.entries[0].direction").value("CREDIT"));

        mvc.perform(get("/transactions/" + id).header("Authorization", "Bearer " + mallory))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Transaction not found"));
    }

    @Test
    void unknownMalformedAndUnauthenticatedTransactionRequests() throws Exception {
        String token = registerAndLogin();

        mvc.perform(get("/transactions/" + UUID.randomUUID()).header("Authorization", "Bearer " + token))
                .andExpect(status().isNotFound());
        mvc.perform(get("/transactions/not-a-uuid").header("Authorization", "Bearer " + token))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/transactions/" + UUID.randomUUID()))
                .andExpect(status().isUnauthorized());
    }

    // ---- helpers (same style as MoneyApiIntegrationTest) ----

    private JsonNode history(String token, long accountId, String query) throws Exception {
        String body = mvc.perform(get("/accounts/" + accountId + "/transactions?" + query)
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(body);
    }

    private static List<Long> amounts(JsonNode page) {
        List<Long> out = new ArrayList<>();
        page.get("items").forEach(n -> out.add(n.get("amount").asLong()));
        return out;
    }

    private void deposit(String token, long accountId, long amount) throws Exception {
        mvc.perform(MockMvcRequestBuilders.post("/accounts/" + accountId + "/deposits")
                        .header("Authorization", "Bearer " + token)
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(APPLICATION_JSON)
                        .content("{\"amount\":" + amount + "}"))
                .andExpect(status().isCreated());
    }

    /** Returns the new transaction's id. */
    private String transfer(String token, long from, long to, long amount) throws Exception {
        String body = mvc.perform(MockMvcRequestBuilders.post("/transfers")
                        .header("Authorization", "Bearer " + token)
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(APPLICATION_JSON)
                        .content("{\"fromAccountId\":" + from + ",\"toAccountId\":" + to + ",\"amount\":" + amount + "}"))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(body).get("id").asText();
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
}