package com.wallet.transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.AbstractIntegrationTest;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestPropertySource(properties = "spring.jpa.properties.hibernate.generate_statistics=true")
class TransactionHistoryQueryCountTest extends AbstractIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired EntityManagerFactory emf;

    @Test
    void historyRunsTheSameNumberOfStatementsWhateverThePageSize() throws Exception {
        String token = registerAndLogin();
        long account = createAccount(token);
        for (int i = 1; i <= 30; i++) {
            deposit(token, account, 100);
        }
        Statistics stats = emf.unwrap(SessionFactory.class).getStatistics();

        long small = statementsFor(stats, token, account, 5);
        long large = statementsFor(stats, token, account, 25);

        // 5 rows vs 25 rows: if the count grew, something is loading per row (N+1)
        assertThat(large).isEqualTo(small);
        // ownership check + the page query; a little slack so this does not break on a harmless extra statement
        assertThat(small).isLessThanOrEqualTo(3);
    }

    private long statementsFor(Statistics stats, String token, long account, int limit) throws Exception {
        stats.clear();
        mvc.perform(get("/accounts/" + account + "/transactions?limit=" + limit)
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(limit)); // proves the page really had `limit` rows
        return stats.getPrepareStatementCount();
    }

    private void deposit(String token, long accountId, long amount) throws Exception {
        mvc.perform(MockMvcRequestBuilders.post("/accounts/" + accountId + "/deposits")
                        .header("Authorization", "Bearer " + token)
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType(APPLICATION_JSON)
                        .content("{\"amount\":%d}".formatted(amount)))
                .andExpect(status().isCreated());
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