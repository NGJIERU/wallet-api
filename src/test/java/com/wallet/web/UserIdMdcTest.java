package com.wallet.web;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.wallet.AbstractIntegrationTest;
import jakarta.servlet.Filter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.web.FilterChainProxy;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/** RQ7 (userId part), RQ8 and the "not double registered" check, in the existing limiter-off context. */
class UserIdMdcTest extends AbstractIntegrationTest {

    private static final String H = RequestIdFilter.HEADER;

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired JwtDecoder jwtDecoder;
    @Autowired ApplicationContext ctx;
    @Autowired FilterChainProxy securityChain;

    private final ListAppender<ILoggingEvent> events = new ListAppender<>();
    private Logger root;

    @BeforeEach
    void attach() {
        root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        events.start();
        root.addAppender(events);
    }

    @AfterEach
    void detach() {
        root.detachAppender(events);
        events.stop();
    }

    private String completion(String requestId) {
        return new ArrayList<>(events.list).stream().map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("request completed") && m.contains("requestId=" + requestId))
                .reduce((a, b) -> { throw new AssertionError("more than one completion line for " + requestId); })
                .orElseThrow(() -> new AssertionError("no completion line for " + requestId));
    }

    private String loginToken() throws Exception {
        String body = "{\"email\":\"mdc-" + UUID.randomUUID() + "@example.test\",\"password\":\"password123\"}";
        mvc.perform(post("/auth/register").contentType(APPLICATION_JSON).content(body)).andReturn();
        String resp = mvc.perform(post("/auth/login").contentType(APPLICATION_JSON).content(body))
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(resp).get("accessToken").asString();
    }

    // RQ8
    @Test
    void rq8_theCompletionLineCarriesThatUsersIdAndNotAnothers() throws Exception {
        String ta = loginToken();
        String tb = loginToken();
        String subA = jwtDecoder.decode(ta).getSubject();
        String subB = jwtDecoder.decode(tb).getSubject();
        assertThat(subA).isNotEqualTo(subB);

        String ida = "rq8a-" + UUID.randomUUID();
        String idb = "rq8b-" + UUID.randomUUID();
        mvc.perform(get("/accounts").header(H, ida).header("Authorization", "Bearer " + ta)).andReturn();
        mvc.perform(get("/accounts").header(H, idb).header("Authorization", "Bearer " + tb)).andReturn();
        assertThat(completion(ida)).contains("status=200", "userId=" + subA).doesNotContain("userId=" + subB);
        assertThat(completion(idb)).contains("status=200", "userId=" + subB).doesNotContain("userId=" + subA);

        // a 4xx for an authenticated user (an account that is not theirs) still carries the user id
        String id4 = "rq8c-" + UUID.randomUUID();
        mvc.perform(get("/accounts/999999999").header(H, id4).header("Authorization", "Bearer " + ta)).andReturn();
        assertThat(completion(id4)).contains("status=404", "userId=" + subA);

        // no token: no user id
        String id0 = "rq8d-" + UUID.randomUUID();
        mvc.perform(get("/accounts").header(H, id0)).andReturn();
        assertThat(completion(id0)).contains("status=401").doesNotContain("userId=");
    }

    // RQ7 (userId part)
    @Test
    void rq7_theUserIdIsNotLeftInTheMdcAfterTheRequest() throws Exception {
        String t = loginToken();
        mvc.perform(get("/accounts").header("Authorization", "Bearer " + t)).andReturn();
        assertThat(MDC.get("userId")).isNull();
        assertThat(MDC.get("requestId")).isNull();
        String id = "rq7-" + UUID.randomUUID();
        mvc.perform(get("/accounts").header(H, id)).andReturn();
        assertThat(completion(id)).doesNotContain("userId=");
    }

    // P5: the in-chain filter is in the chain exactly once and is not a servlet-filter bean
    @Test
    void theInChainFilterIsRegisteredOnceAndIsNotABean() {
        long inChain = securityChain.getFilterChains().stream()
                .flatMap(c -> c.getFilters().stream()).filter(f -> f instanceof UserIdMdcFilter).count();
        assertThat(inChain).isEqualTo(1);
        List<Filter> beans = new ArrayList<>(ctx.getBeansOfType(Filter.class).values());
        assertThat(beans).noneMatch(f -> f instanceof UserIdMdcFilter);
        assertThat(beans.stream().filter(f -> f instanceof RequestIdFilter).count()).isEqualTo(1);
    }
}
