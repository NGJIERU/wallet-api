package com.wallet.web;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.wallet.AbstractIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/** RQ1 to RQ4, RQ6, RQ7 (request-id part): the outer filter in the existing limiter-off context. */
class RequestIdTest extends AbstractIntegrationTest {

    private static final String H = RequestIdFilter.HEADER;

    @Autowired MockMvc mvc;

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

    private List<String> messages() {
        return new ArrayList<>(events.list).stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<String> completions() {
        return messages().stream().filter(m -> m.startsWith("request completed")).toList();
    }

    // RQ1
    @Test
    void rq1_aGeneratedIdIsReturnedWhenNoneIsSupplied() throws Exception {
        MvcResult r = mvc.perform(get("/accounts")).andReturn();
        String id = r.getResponse().getHeader(H);
        assertThat(id).isNotNull();
        assertThat(UUID.fromString(id).toString()).isEqualTo(id);
    }

    // RQ2
    @Test
    void rq2_aValidSuppliedIdIsEchoedUnchanged() throws Exception {
        String id = "client.id_1-ABC";
        MvcResult r = mvc.perform(get("/accounts").header(H, id)).andReturn();
        assertThat(r.getResponse().getHeader(H)).isEqualTo(id);
        assertThat(completions()).filteredOn(m -> m.contains("requestId=" + id)).hasSize(1);
    }

    // RQ3
    @Test
    void rq3_unsafeSuppliedIdsAreReplacedAndNeverLogged() throws Exception {
        String[] unsafe = {"a b", "x\nINJECTED line", "a\u0001b", "a".repeat(65), "", "bad/slash", "id;drop"};
        int before = completions().size();
        for (String v : unsafe) {
            MvcResult r = mvc.perform(get("/accounts").header(H, v)).andReturn();
            String got = r.getResponse().getHeader(H);
            assertThat(got).isNotEqualTo(v).matches("[A-Za-z0-9._-]{1,64}");
        }
        assertThat(completions()).as("exactly one completion line per request").hasSize(before + unsafe.length);
        String all = String.join("\n", messages());
        for (String v : unsafe) {
            if (!v.isEmpty()) {
                assertThat(all).as("unsafe value must not be logged").doesNotContain(v);
            }
        }
        assertThat(messages()).noneMatch(m -> m.contains("INJECTED"));
    }

    // RQ4 (401 is wrapped by the outer filter)
    @Test
    void rq4_aRequestWithoutATokenGives401WithHeaderAndExactlyOneCompletionLine() throws Exception {
        String id = "rq4-" + UUID.randomUUID();
        MvcResult r = mvc.perform(get("/accounts").header(H, id)).andReturn();
        assertThat(r.getResponse().getStatus()).isEqualTo(401);
        assertThat(r.getResponse().getHeader(H)).isEqualTo(id);
        assertThat(completions()).filteredOn(m -> m.contains("requestId=" + id)).singleElement()
                .satisfies(m -> assertThat(m).contains("status=401"));
    }

    // RQ6 (content; the authenticated user-id part is in 7.1c)
    @Test
    void rq6_completionLineHasTheFieldsAndLeaksNothing() throws Exception {
        String id = "rq6-" + UUID.randomUUID();
        mvc.perform(get("/accounts?cursor=query-secret-123").header(H, id)).andReturn();
        String line = completions().stream().filter(m -> m.contains("requestId=" + id)).findFirst().orElseThrow();
        assertThat(line).contains("method=GET", "path=/accounts", "status=401", "durationMs=", "requestId=" + id)
                .doesNotContain("userId=", "query-secret-123", "cursor");

        String id2 = "rq6b-" + UUID.randomUUID();
        mvc.perform(post("/accounts/1/deposits").header(H, id2)
                .header("Authorization", "Bearer garbage-token-xyz")
                .header("Idempotency-Key", "idem-secret-42")
                .contentType(APPLICATION_JSON).content("{\"note\":\"body-secret-marker\"}")).andReturn();
        String all = String.join("\n", messages());
        assertThat(all).doesNotContain("garbage-token-xyz", "idem-secret-42", "body-secret-marker");

        String idH = "rq6h-" + UUID.randomUUID();
        MvcResult health = mvc.perform(get("/actuator/health").header(H, idH)).andReturn();
        assertThat(health.getResponse().getHeader(H)).isEqualTo(idH);
        assertThat(completions()).noneMatch(m -> m.contains("requestId=" + idH));
    }

    // RQ7 (request-id part; userId part in 7.1c)
    @Test
    void rq7_theMdcIsClearAfterTheRequestAndIdsAreNotShared() throws Exception {
        String a = mvc.perform(get("/accounts")).andReturn().getResponse().getHeader(H);
        assertThat(MDC.get("requestId")).isNull();
        assertThat(MDC.get("userId")).isNull();
        String b = mvc.perform(get("/accounts")).andReturn().getResponse().getHeader(H);
        assertThat(MDC.get("requestId")).isNull();
        assertThat(a).isNotEqualTo(b);
    }
}
