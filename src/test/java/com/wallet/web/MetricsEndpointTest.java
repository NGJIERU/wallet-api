package com.wallet.web;

import com.wallet.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

import static org.hamcrest.Matchers.hasItems;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** GA4 and GA5: /actuator/metrics is authenticated and lists the wallet.* meters (existing limiter-off context). */
class MetricsEndpointTest extends AbstractIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;

    private String token() throws Exception {
        String body = "{\"email\":\"metrics-" + UUID.randomUUID() + "@example.test\",\"password\":\"password123\"}";
        mvc.perform(post("/auth/register").contentType(APPLICATION_JSON).content(body)).andExpect(status().isCreated());
        String resp = mvc.perform(post("/auth/login").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        return mapper.readTree(resp).get("accessToken").asString();
    }

    // GA4
    @Test
    void ga4_metricsNeedsATokenAndListsTheWalletMeters() throws Exception {
        mvc.perform(get("/actuator/metrics")).andExpect(status().isUnauthorized());
        mvc.perform(get("/actuator/metrics").header("Authorization", "Bearer " + token()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.names", hasItems("wallet.outbox.unpublished", "wallet.outbox.oldest.age.seconds")));
    }

    // GA5 (a gauge cannot be asserted at 0 in the shared database; counters at 0 are checked in 7.2b)
    @Test
    void ga5_aSingleNamedMeterIsReadable() throws Exception {
        mvc.perform(get("/actuator/metrics/wallet.outbox.unpublished")).andExpect(status().isUnauthorized());
        mvc.perform(get("/actuator/metrics/wallet.outbox.unpublished").header("Authorization", "Bearer " + token()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("wallet.outbox.unpublished"))
                .andExpect(jsonPath("$.measurements[0].statistic").value("VALUE"));
    }
}
