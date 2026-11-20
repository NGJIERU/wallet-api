package com.wallet.ratelimit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.wallet.web.RequestIdFilter;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.test.web.servlet.MvcResult;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** RQ5: a 429 is wrapped by the outer filter (limiter-on context, the existing 7th). */
class RequestIdRateLimitTest extends AbstractRateLimitIntegrationTest {

    @Test
    void rq5_a429HasTheHeaderAndExactlyOneCompletionLine() throws Exception {
        String token = registerAndLogin();
        long accountId = createAccount(token);
        for (int i = 0; i < 5; i++) {
            deposit(token, accountId, key()).andExpect(status().isCreated());
        }
        String id = "rq5-" + UUID.randomUUID();
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ListAppender<ILoggingEvent> events = new ListAppender<>();
        events.start();
        root.addAppender(events);
        try {
            MvcResult r = mvc.perform(post("/accounts/" + accountId + "/deposits")
                            .header("Authorization", "Bearer " + token)
                            .header("Idempotency-Key", key())
                            .header(RequestIdFilter.HEADER, id)
                            .contentType(APPLICATION_JSON).content("{\"amount\":100}"))
                    .andExpect(status().isTooManyRequests()).andReturn();
            assertThat(r.getResponse().getHeader(RequestIdFilter.HEADER)).isEqualTo(id);
            List<String> lines = new ArrayList<>(events.list).stream().map(ILoggingEvent::getFormattedMessage)
                    .filter(m -> m.startsWith("request completed") && m.contains("requestId=" + id)).toList();
            assertThat(lines).singleElement().satisfies(m -> assertThat(m).contains("status=429"));
        } finally {
            root.detachAppender(events);
            events.stop();
        }
    }
}
