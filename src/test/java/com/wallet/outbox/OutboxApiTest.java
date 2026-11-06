package com.wallet.outbox;

import com.wallet.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.UUID;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** T12: events exist with the right payload when money moves through the real controllers. */
class OutboxApiTest extends AbstractIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcClient jdbc;

    @Test
    void depositThenTransferThroughHttpWriteTheExpectedEvents() throws Exception {
        String aliceToken = registerAndLogin();
        String bobToken = registerAndLogin();
        long aliceAccount = createAccount(aliceToken);
        long bobAccount = createAccount(bobToken);
        long aliceUser = ownerOf(aliceAccount);
        long bobUser = ownerOf(bobAccount);

        // deposit
        String depositKey = key();
        String depositBody = post("/accounts/" + aliceAccount + "/deposits", aliceToken, depositKey, "{\"amount\":1000}")
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        String depositTxn = mapper.readTree(depositBody).get("id").asString();

        List<JsonNode> aliceEvents = events(aliceAccount);
        assertThat(aliceEvents).hasSize(1);
        JsonNode dep = aliceEvents.get(0);
        assertThat(dep.get("transactionId").asString()).isEqualTo(depositTxn);
        assertThat(dep.get("direction").asString()).isEqualTo("CREDIT");
        assertThat(dep.get("transactionType").asString()).isEqualTo("DEPOSIT");
        assertThat(dep.get("amount").asLong()).isEqualTo(1000);
        assertThat(dep.get("ownerUserId").asLong()).isEqualTo(aliceUser);
        assertThat(dep.get("accountId").asLong()).isEqualTo(aliceAccount);

        // same-key replay over HTTP writes nothing new
        post("/accounts/" + aliceAccount + "/deposits", aliceToken, depositKey, "{\"amount\":1000}")
                .andExpect(status().isCreated());
        assertThat(events(aliceAccount)).hasSize(1);

        // transfer to another user
        String transferKey = key();
        String transferJson = "{\"fromAccountId\":" + aliceAccount + ",\"toAccountId\":" + bobAccount + ",\"amount\":400}";
        String transferBody = post("/transfers", aliceToken, transferKey, transferJson)
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        String transferTxn = mapper.readTree(transferBody).get("id").asString();

        List<JsonNode> forTransfer = eventsForTxn(transferTxn);
        assertThat(forTransfer).hasSize(2);
        JsonNode debit = forTransfer.get(0);
        JsonNode credit = forTransfer.get(1);
        assertThat(debit.get("direction").asString()).isEqualTo("DEBIT");
        assertThat(debit.get("accountId").asLong()).isEqualTo(aliceAccount);
        assertThat(debit.get("ownerUserId").asLong()).isEqualTo(aliceUser);
        assertThat(credit.get("direction").asString()).isEqualTo("CREDIT");
        assertThat(credit.get("accountId").asLong()).isEqualTo(bobAccount);
        assertThat(credit.get("ownerUserId").asLong()).isEqualTo(bobUser);
        for (JsonNode e : forTransfer) {
            assertThat(e.get("transactionType").asString()).isEqualTo("TRANSFER");
            assertThat(e.get("amount").asLong()).isEqualTo(400);
        }

        // replay of the transfer writes nothing new
        post("/transfers", aliceToken, transferKey, transferJson).andExpect(status().isCreated());
        assertThat(eventsForTxn(transferTxn)).hasSize(2);
        assertThat(events(aliceAccount)).hasSize(2); // deposit + debit
        assertThat(events(bobAccount)).hasSize(1);   // credit
        assertReconciled(jdbc, aliceAccount, bobAccount);
    }

    private List<JsonNode> events(long accountId) {
        return jdbc.sql("SELECT payload::text FROM outbox_events WHERE aggregate_id = ? ORDER BY id")
                .param(String.valueOf(accountId))
                .query((rs, n) -> mapper.readTree(rs.getString(1))).list();
    }

    private List<JsonNode> eventsForTxn(String txnId) {
        return jdbc.sql("SELECT payload::text FROM outbox_events WHERE payload->>'transactionId' = ? ORDER BY id")
                .param(txnId)
                .query((rs, n) -> mapper.readTree(rs.getString(1))).list();
    }

    private long ownerOf(long accountId) {
        return jdbc.sql("SELECT user_id FROM accounts WHERE id = ?").param(accountId).query(Long.class).single();
    }

    private ResultActions post(String path, String token, String key, String body) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.post(path)
                .header("Authorization", "Bearer " + token)
                .header("Idempotency-Key", key)
                .contentType(APPLICATION_JSON)
                .content(body));
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
        return mapper.readTree(response).get("accessToken").asString();
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
