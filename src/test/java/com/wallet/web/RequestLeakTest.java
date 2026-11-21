package com.wallet.web;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.wallet.AbstractIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import com.wallet.account.AccountRepository;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.test.web.servlet.ResultActions;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** RQ9 (part 1): reachable error paths leak no email, password, Idempotency-Key value or token into the log. */
class RequestLeakTest extends AbstractIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired AccountRepository accounts;
    @Autowired PlatformTransactionManager txManager;

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

    /** Message plus any throwable text of every captured event, lower-cased. */
    private String log() {
        StringBuilder sb = new StringBuilder();
        for (ILoggingEvent e : new ArrayList<>(events.list)) {
            sb.append(e.getFormattedMessage()).append('\n');
            if (e.getThrowableProxy() != null) {
                sb.append(e.getThrowableProxy().getClassName()).append(' ').append(e.getThrowableProxy().getMessage()).append('\n');
            }
        }
        return sb.toString().toLowerCase();
    }

    private void assertNoneOf(String... secrets) {
        String all = log();
        assertThat(all).as("log must not be empty (completion lines expected)").contains("request completed");
        org.assertj.core.api.SoftAssertions soft = new org.assertj.core.api.SoftAssertions();
        for (String s : secrets) {
            String shown = s.substring(0, Math.min(8, s.length())) + "...";
            boolean found = all.contains(s.toLowerCase());
            System.out.println("[RQ9 captured-log check secret=" + shown + " found=" + found + "]");
            soft.assertThat(all).as("leaked: " + shown).doesNotContain(s.toLowerCase());
        }
        soft.assertAll();
    }

    private static String body(String email, String password) {
        return "{\"email\":\"" + email + "\",\"password\":\"" + password + "\"}";
    }

    private ResultActions postJson(String path, String json) throws Exception {
        return mvc.perform(post(path).contentType(APPLICATION_JSON).content(json));
    }

    private String token(String email, String password) throws Exception {
        postJson("/auth/register", body(email, password)).andExpect(status().isCreated());
        String resp = postJson("/auth/login", body(email, password)).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(resp).get("accessToken").asString();
    }

    private ResultActions money(String token, long accountId, String kind, String key, int amount) throws Exception {
        return mvc.perform(post("/accounts/" + accountId + "/" + kind)
                .header("Authorization", "Bearer " + token).header("Idempotency-Key", key)
                .contentType(APPLICATION_JSON).content("{\"amount\":" + amount + "}"));
    }

    private long account(String token) throws Exception {
        String r = mvc.perform(post("/accounts").header("Authorization", "Bearer " + token)
                        .contentType(APPLICATION_JSON).content("{\"currency\":\"SGD\"}"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        return mapper.readTree(r).get("id").asLong();
    }

    @Test
    void rq9_duplicateEmailRegistration() throws Exception {
        String email = "leak-" + UUID.randomUUID() + "@example.test";
        String pw = "leakpw" + UUID.randomUUID().toString().substring(0, 8);
        postJson("/auth/register", body(email, pw)).andExpect(status().isCreated());
        postJson("/auth/register", body(email.toUpperCase(), pw)).andExpect(status().isConflict());
        assertNoneOf(email, pw);
    }

    @Test
    void rq9_wrongPasswordLogin() throws Exception {
        String email = "leak-" + UUID.randomUUID() + "@example.test";
        String pw = "leakpw" + UUID.randomUUID().toString().substring(0, 8);
        String wrong = "wrongpw" + UUID.randomUUID().toString().substring(0, 8);
        postJson("/auth/register", body(email, pw)).andExpect(status().isCreated());
        postJson("/auth/login", body(email, wrong)).andExpect(status().isUnauthorized());
        assertNoneOf(email, pw, wrong);
    }

    @Test
    void rq9_idempotencyKeyReuseAndInsufficientFunds() throws Exception {
        String email = "leak-" + UUID.randomUUID() + "@example.test";
        String pw = "leakpw" + UUID.randomUUID().toString().substring(0, 8);
        String token = token(email, pw);
        long acc = account(token);
        String k1 = "keyreuse-" + UUID.randomUUID();
        String k2 = "nofunds-" + UUID.randomUUID();
        money(token, acc, "deposits", k1, 100).andExpect(status().isCreated());
        money(token, acc, "deposits", k1, 200).andExpect(status().isUnprocessableContent()); // key reuse, other body
        money(token, acc, "withdrawals", k2, 999_999).andExpect(status().isUnprocessableContent()); // insufficient funds
        String[] parts = token.split("\\.");
        assertNoneOf(email, pw, k1, k2, token, parts[0], parts[1], parts[2]);
    }

    // RQ9 part 2: a lock timeout on the account row (409) leaks nothing
    @Test
    void rq9_lockTimeoutIs409() throws Exception {
        String email = "leak-" + UUID.randomUUID() + "@example.test";
        String pw = "leakpw" + UUID.randomUUID().toString().substring(0, 8);
        String token = token(email, pw);
        long acc = account(token);
        String k = "busy-" + UUID.randomUUID();

        TransactionTemplate tx = new TransactionTemplate(txManager);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> holder = pool.submit(() -> tx.executeWithoutResult(st -> {
            accounts.lockAllByIdOrdered(List.of(acc));
            locked.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        try {
            assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();
            money(token, acc, "deposits", k, 100).andExpect(status().isConflict()); // waits for the lock timeout
        } finally {
            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
            pool.shutdown();
        }
        String[] parts = token.split("\\.");
        assertNoneOf(email, pw, k, token, parts[0], parts[1], parts[2]);
    }
}
