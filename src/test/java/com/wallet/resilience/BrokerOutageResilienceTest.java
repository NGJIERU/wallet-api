package com.wallet.resilience;

import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.idempotency.StoredResponse;
import com.wallet.transaction.MoneyService;
import com.wallet.transaction.TransferService;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * FT1-FT4: the whole chain (money endpoints, outbox, relay, Kafka, consumer, notifications) against a broker that is
 * HUNG (docker pause: the port accepts TCP connections, nothing answers) before the context starts, then unpaused.
 * Owns its broker; never touches the shared one. Sixth Spring context, closed after the class so its relay and
 * consumer do not outlive it. One sequential scenario because the phases are one outage; every assertion names its phase.
 * All waits are bounded loops with deadlines.
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "app.outbox.relay.enabled=true",
        "app.outbox.relay.poll-interval=100ms",
        "app.outbox.relay.backoff-min=200ms",
        "app.outbox.relay.backoff-max=2s",
        "app.notifications.consumer.enabled=true",
        "app.notifications.consumer.start-backoff-min=200ms",
        "app.notifications.consumer.start-backoff-max=2s",
        "app.notifications.consumer-group=resilience-${random.uuid}",
        "app.notifications.retry.initial-backoff=50ms",
        "app.lock-timeout-ms=30000"})
class BrokerOutageResilienceTest extends AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(BrokerOutageResilienceTest.class);
    private static final long STARTUP_BOUND_MS = 90_000;

    private static final KafkaContainer OWN_KAFKA = new KafkaContainer("apache/kafka:4.2.2")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    private static volatile long pausedAtNanos;

    @BeforeAll
    static void startAndPauseOwnBroker() {
        OWN_KAFKA.start();
        pause();
        pausedAtNanos = System.nanoTime();
    }

    @AfterAll
    static void unpauseOwnBroker() {
        try {
            if (isPaused()) {
                unpause();
            }
        } catch (RuntimeException e) {
            log.warn("[RESILIENCE] could not unpause the own broker: {}", e.toString());
        }
    }

    // The base class registers spring.kafka.bootstrap-servers for the shared broker and wins over a second
    // registration of the same key; Boot has no admin-specific bootstrap-servers property. Boot 4 builds the producer,
    // consumer and admin from one KafkaConnectionDetails bean (its own is @ConditionalOnMissingBean), so this bean
    // points all three clients at the own broker. The FT1 assertions check the resulting factories.
    @TestConfiguration
    static class OwnBrokerConnection {
        @Bean
        KafkaConnectionDetails ownBrokerConnectionDetails() {
            return () -> List.of(OWN_KAFKA.getBootstrapServers());
        }
    }

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired MoneyService money;
    @Autowired TransferService transfers;
    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired JdbcClient jdbc;
    @Autowired ProducerFactory<String, String> producerFactory;
    @Autowired ConsumerFactory<String, String> consumerFactory;
    @Autowired KafkaAdmin kafkaAdmin;

    @Test
    void ft1ToFt4_theChainSurvivesABrokerThatIsHungThenReturns() throws Exception {
        long startupMs = (System.nanoTime() - pausedAtNanos) / 1_000_000;

        // ---- FT1: the context started while the broker was hung
        String own = OWN_KAFKA.getBootstrapServers();
        assertThat(String.valueOf(producerFactory.getConfigurationProperties().get("bootstrap.servers")))
                .as("FT1: producer must use the test's own broker, not the shared one").contains(own);
        assertThat(String.valueOf(consumerFactory.getConfigurationProperties().get("bootstrap.servers")))
                .as("FT1: consumer must use the test's own broker").contains(own);
        assertThat(String.valueOf(kafkaAdmin.getConfigurationProperties().get("bootstrap.servers")))
                .as("FT1: admin must use the test's own broker").contains(own);
        assertThat(isPaused()).as("FT1: broker container is paused").isTrue();
        assertThat(tcpConnects()).as("FT1: a paused broker still accepts TCP connections on the mapped port").isTrue();
        assertThat(brokerAnswers(4)).as("FT1: a paused broker never answers a Kafka request").isFalse();
        assertThat(startupMs).as("FT1: start-up with a hung broker stays within the bound").isLessThan(STARTUP_BOUND_MS);
        log.info("[RESILIENCE FT1] pause-to-first-test-statement {} ms (includes context start-up), bound {} ms", startupMs, STARTUP_BOUND_MS);
        health("FT1/FT4 start-up, broker hung");

        // ---- FT2: money endpoints while the broker is still hung
        // UNIQUE (user_id, currency): one SGD account per user, so five users
        List<String> tokens = new ArrayList<>();
        List<Long> httpAccounts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String t = registerAndLogin();
            tokens.add(t);
            httpAccounts.add(createAccount(t));
        }
        long t0 = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(10);
        try {
            List<Future<Integer>> fs = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                long accountId = httpAccounts.get(i % 5);
                String token = tokens.get(i % 5);
                fs.add(pool.submit(() -> deposit(token, accountId, key(), "{\"amount\":100}")
                        .andReturn().getResponse().getStatus()));
            }
            for (Future<Integer> f : fs) {
                assertThat(f.get(60, TimeUnit.SECONDS)).as("FT2: parallel deposit while the broker is hung").isEqualTo(201);
            }
        } finally {
            pool.shutdownNow();
        }
        long depositsMs = (System.nanoTime() - t0) / 1_000_000;
        assertThat(depositsMs).as("FT2: 20 parallel deposits are not blocked by Kafka").isLessThan(30_000);

        Wallet a = wallet();
        Wallet b = wallet();
        money.deposit(a.userId(), a.accountId(), key(), 10_000);
        StoredResponse transfer = transfers.transfer(a.userId(), key(), a.accountId(), b.accountId(), 2_500);
        assertThat(transfer.code()).as("FT2: transfer while the broker is hung").isEqualTo(201);

        List<Long> ids = new ArrayList<>(httpAccounts);
        ids.add(a.accountId());
        ids.add(b.accountId());
        List<String> sids = ids.stream().map(String::valueOf).toList();
        long expected = 20 + 1 + 2; // 20 deposits, 1 funding deposit, 1 transfer (debit + credit)
        assertThat(ledgerEntries(ids)).as("FT2: ledger entries").isEqualTo(expected);
        assertThat(outboxRows(sids, false)).as("FT2: one outbox row per ledger entry").isEqualTo(expected);
        assertThat(outboxRows(sids, true)).as("FT2: every row still unpublished").isEqualTo(expected);
        assertThat(notifications(ids)).as("FT2: no notification yet").isZero();
        assertThat(isPaused()).as("FT2: broker still paused").isTrue();
        log.info("[RESILIENCE FT2] 20 parallel deposits in {} ms, ledger={} outbox={} unpublished={} notifications=0",
                depositsMs, expected, expected, expected);
        health("FT4 during the outage, rows waiting");

        // ---- FT3: unpause; nothing is restarted
        long t1 = System.nanoTime();
        unpause();
        assertThat(isPaused()).as("FT3: broker unpaused").isFalse();
        await(() -> brokerAnswers(3), 60, "FT3: the broker did not answer after the unpause");
        await(() -> outboxRows(sids, true) == 0, 120, "FT3: unpublished rows did not drain");
        long drainedMs = (System.nanoTime() - t1) / 1_000_000;
        await(() -> notifications(ids) >= expected, 120, "FT3: notifications did not reach " + expected);
        long consumedMs = (System.nanoTime() - t1) / 1_000_000;
        assertThat(notifications(ids)).as("FT3: exactly one notification per ledger entry").isEqualTo(expected);
        assertThat(distinctNotificationLedgerEntries(ids)).as("FT3: no duplicate notification").isEqualTo(expected);
        assertThat(outboxRows(sids, false)).as("FT3: outbox rows unchanged in number").isEqualTo(expected);
        log.info("[RESILIENCE FT3] unpause to all rows published {} ms, to all notifications {} ms, notifications={} distinct={}",
                drainedMs, consumedMs, expected, expected);
        log.info("[RESILIENCE FT3] dead-letter topic present after the outage: {}", topicExists("wallet.transaction-completed.v1-dlt"));

        // ---- FT4: after
        health("FT4 after the outage, drained");
    }

    // ---- broker control and probes

    private static void pause() {
        OWN_KAFKA.getDockerClient().pauseContainerCmd(OWN_KAFKA.getContainerId()).exec();
    }

    private static void unpause() {
        OWN_KAFKA.getDockerClient().unpauseContainerCmd(OWN_KAFKA.getContainerId()).exec();
    }

    private static boolean isPaused() {
        return Boolean.TRUE.equals(OWN_KAFKA.getDockerClient().inspectContainerCmd(OWN_KAFKA.getContainerId())
                .exec().getState().getPaused());
    }

    private static boolean tcpConnects() {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(OWN_KAFKA.getHost(), OWN_KAFKA.getFirstMappedPort()), 3000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean brokerAnswers(int seconds) {
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, OWN_KAFKA.getBootstrapServers(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, seconds * 1000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, seconds * 1000))) {
            admin.listTopics().names().get(seconds + 2L, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean topicExists(String topic) {
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, OWN_KAFKA.getBootstrapServers(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000))) {
            return admin.listTopics().names().get(7, TimeUnit.SECONDS).contains(topic);
        } catch (Exception e) {
            return false;
        }
    }

    // ---- database probes, scoped to this test's accounts (Postgres is shared with the whole suite)

    private long ledgerEntries(List<Long> ids) {
        return jdbc.sql("SELECT COUNT(*) FROM ledger_entries WHERE account_id IN (:ids)")
                .param("ids", ids).query(Long.class).single();
    }

    private long outboxRows(List<String> sids, boolean unpublishedOnly) {
        String sql = "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id IN (:ids)"
                + (unpublishedOnly ? " AND published_at IS NULL" : "");
        return jdbc.sql(sql).param("ids", sids).query(Long.class).single();
    }

    private long notifications(List<Long> ids) {
        return jdbc.sql("SELECT COUNT(*) FROM notifications WHERE account_id IN (:ids)")
                .param("ids", ids).query(Long.class).single();
    }

    private long distinctNotificationLedgerEntries(List<Long> ids) {
        return jdbc.sql("SELECT COUNT(DISTINCT ledger_entry_id) FROM notifications WHERE account_id IN (:ids)")
                .param("ids", ids).query(Long.class).single();
    }

    // ---- HTTP helpers (same shape as MoneyApiIntegrationTest)

    private void health(String phase) throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
        log.info("[RESILIENCE FT4] health UP: {}", phase);
    }

    private ResultActions deposit(String token, long accountId, String key, String body) throws Exception {
        return mvc.perform(MockMvcRequestBuilders.post("/accounts/" + accountId + "/deposits")
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

    private record Wallet(long userId, long accountId) {
    }

    private Wallet wallet() {
        User u = users.saveAndFlush(new User("resilience-" + UUID.randomUUID() + "@example.test", "x"));
        Account acc = accounts.saveAndFlush(new Account(u.getId(), "SGD"));
        return new Wallet(u.getId(), acc.getId());
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }

    private static void await(BooleanSupplier cond, int seconds, String message) {
        long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(message + " within " + seconds + " s");
            }
            LockSupport.parkNanos(Duration.ofMillis(200).toNanos());
        }
    }
}
