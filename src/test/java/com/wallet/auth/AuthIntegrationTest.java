package com.wallet.auth;

import com.wallet.AbstractIntegrationTest;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AuthIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    MockMvc mvc;
    @Autowired
    UserRepository users;

    @Test
    void registerStoresBcryptHashNotPlainPassword() throws Exception {
        String email = uniqueEmail();

        register(email, "password123")
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.email").value(email));

        String stored = users.findByEmail(email).orElseThrow().getPasswordHash();
        assertThat(stored).isNotEqualTo("password123");
        assertThat(stored).startsWith("$2");
    }

    @Test
    void registerDuplicateEmailInDifferentCaseReturns409() throws Exception {
        String email = uniqueEmail();
        register(email, "password123").andExpect(status().isCreated());

        register(email.toUpperCase(), "password123").andExpect(status().isConflict());
    }

    @Test
    void registerWithBadEmailOrShortPasswordReturns400() throws Exception {
        register("not-an-email", "password123").andExpect(status().isBadRequest());
        register(uniqueEmail(), "short").andExpect(status().isBadRequest());
    }

    @Test
    void loginWithCorrectPasswordReturnsToken() throws Exception {
        String email = uniqueEmail();
        register(email, "password123").andExpect(status().isCreated());

        login(email, "password123")
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accessToken").isNotEmpty())
                .andExpect(jsonPath("$.tokenType").value("Bearer"));
    }

    @Test
    void wrongPasswordAndUnknownEmailGiveIdentical401() throws Exception {
        String email = uniqueEmail();
        register(email, "password123").andExpect(status().isCreated());

        String wrongPassword = login(email, "wrongpassword")
                .andExpect(status().isUnauthorized())
                .andReturn().getResponse().getContentAsString();
        String unknownEmail = login(uniqueEmail(), "password123")
                .andExpect(status().isUnauthorized())
                .andReturn().getResponse().getContentAsString();

        assertThat(wrongPassword).isEqualTo(unknownEmail);
    }

    @Test
    void missingOrTamperedTokenReturns401() throws Exception {
        mvc.perform(get("/accounts")).andExpect(status().isUnauthorized());

        String email = uniqueEmail();
        register(email, "password123").andExpect(status().isCreated());
        String body = login(email, "password123").andReturn().getResponse().getContentAsString();
        String token = body.replaceAll(".*\"accessToken\":\"([^\"]+)\".*", "$1");
        String tampered = token.substring(0, token.lastIndexOf('.') + 1) + "AAAA";

        mvc.perform(get("/accounts").header("Authorization", "Bearer " + tampered))
                .andExpect(status().isUnauthorized());
    }

    private ResultActions register(String email, String password) throws Exception {
        return mvc.perform(post("/auth/register").contentType(APPLICATION_JSON).content(json(email, password)));
    }

    private ResultActions login(String email, String password) throws Exception {
        return mvc.perform(post("/auth/login").contentType(APPLICATION_JSON).content(json(email, password)));
    }

    private static String json(String email, String password) {
        return "{\"email\":\"" + email + "\",\"password\":\"" + password + "\"}";
    }

    private static String uniqueEmail() {
        return "user-" + UUID.randomUUID() + "@example.test";
    }
}