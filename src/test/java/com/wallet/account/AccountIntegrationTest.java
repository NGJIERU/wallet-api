package com.wallet.account;

import com.wallet.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import java.util.UUID;

import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AccountIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    MockMvc mvc;

    @Test
    void createAccountThenDuplicateCurrencyReturns409() throws Exception {
        String token = registerAndLogin();

        createAccount(token, "SGD")
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.currency").value("SGD"))
                .andExpect(jsonPath("$.balance").value(0));

        createAccount(token, "SGD").andExpect(status().isConflict());
    }

    @Test
    void listReturnsOnlyTheCallersAccounts() throws Exception {
        String tokenA = registerAndLogin();
        String tokenB = registerAndLogin();
        createAccount(tokenA, "SGD").andExpect(status().isCreated());
        createAccount(tokenA, "USD").andExpect(status().isCreated());
        createAccount(tokenB, "MYR").andExpect(status().isCreated());

        mvc.perform(get("/accounts").header("Authorization", "Bearer " + tokenA))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
        mvc.perform(get("/accounts").header("Authorization", "Bearer " + tokenB))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].currency").value("MYR"));
    }

    @Test
    void userCannotReadAnotherUsersAccountAndGets404() throws Exception {
        String tokenA = registerAndLogin();
        String tokenB = registerAndLogin();
        String created = createAccount(tokenA, "SGD")
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String accountId = created.replaceAll(".*\"id\":(\\d+).*", "$1");

        // owner can read it
        mvc.perform(get("/accounts/" + accountId).header("Authorization", "Bearer " + tokenA))
                .andExpect(status().isOk());
        // another user gets 404, same as a missing id, so ids can't be probed
        mvc.perform(get("/accounts/" + accountId).header("Authorization", "Bearer " + tokenB))
                .andExpect(status().isNotFound());
    }

    @Test
    void invalidCurrencyReturns400() throws Exception {
        String token = registerAndLogin();

        createAccount(token, "XXX1").andExpect(status().isBadRequest());   // fails the format check
        createAccount(token, "ZZZ").andExpect(status().isBadRequest());    // valid format, not ISO 4217
    }

    private ResultActions createAccount(String token, String currency) throws Exception {
        return mvc.perform(post("/accounts")
                .header("Authorization", "Bearer " + token)
                .contentType(APPLICATION_JSON)
                .content("{\"currency\":\"" + currency + "\"}"));
    }

    private String registerAndLogin() throws Exception {
        String email = "user-" + UUID.randomUUID() + "@example.test";
        String body = "{\"email\":\"" + email + "\",\"password\":\"password123\"}";
        mvc.perform(post("/auth/register").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isCreated());
        String response = mvc.perform(post("/auth/login").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return response.replaceAll(".*\"accessToken\":\"([^\"]+)\".*", "$1");
    }
}