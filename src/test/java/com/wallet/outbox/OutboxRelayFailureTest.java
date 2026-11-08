package com.wallet.outbox;

import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.config.KafkaTopicConfig;
import com.wallet.transaction.MoneyService;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** RT4 to RT9: failure semantics of the relay. Every test starts from a drained outbox and restores what it touches. */
class OutboxRelayFailureTest extends AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayFailureTest.class);
    private static final String TOPIC = KafkaTopicConfig.TRANSACTION_COMPLETED_TOPIC;

    @Autowired MoneyService money;
    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired JdbcClient jdbc;
    @Autowired OutboxRelay relay;
    @Autowired OutboxSender sender;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired NewTopic topicDeclaration;
    @Autowired DataSource dataSource;
    @Value("${spring.kafka.bootstrap-servers}") String bootstrap;

    private record Wallet(long userId, long accountId) {
    }

    /** Wraps the real sender; fails on one chosen row id and remembers every row it was asked to send. */
    private static final class FailingSender implements OutboxSender {
        final OutboxSender real;
        volatile long failOnId = -1;
        final List<Long> attempted = new CopyOnWriteArrayList<>();

        FailingSender(OutboxSender real) {
            this.real = real;
        }

        @Override
        public void send(OutboxRelay.Row row) throws Exception {
            attempted.add(row.id());
            if (row.id() == failOnId) {
                throw new IllegalStateException("injected send failure");
            }
            real.send(row);
        }

        @Override
        public void prepareRetry() throws Exception {
            real.prepareRetry();
        }
    }

    // RT4
    @Test
    void anUnreachableBrokerMarksNothingAndNothingIsLost() {
        RelaySupport.drain(relay);
        Wallet w = wallet();
        String key = String.valueOf(w.accountId());
        deposits(w, 3);
        List<Long> ids = outboxIds(key);
        assertThat(ids).hasSize(3);

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.255.255.1:9092");
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2000);
        cfg.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(cfg);
        try {
            OutboxRelay dead = new OutboxRelay(jdbc,
                    new KafkaOutboxSender(new KafkaTemplate<>(pf), TOPIC, 3000, kafkaAdmin, topicDeclaration), 100);
            long t0 = System.nanoTime();
            OutboxRelay.BatchResult r = dead.publishBatch();
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.info("[RELAY RT4] unreachable broker: failedAfterMs={} published={} selected={}", ms, r.published(), r.selected());
            assertThat(r.failure()).isNotNull();
            assertThat(r.published()).isZero();
            assertThat(r.selected()).isEqualTo(3);
            assertThat(r.failedRow().id()).isEqualTo(ids.get(0));
            assertThat(ms).as("bounded").isLessThan(15_000);
            assertThat(unpublished(key)).isEqualTo(3);
        } finally {
            pf.destroy();
        }

        RelaySupport.drain(relay);
        List<RelaySupport.Rec> recs = RelaySupport.read(bootstrap, TOPIC, Set.of(key));
        assertThat(recs.stream().map(RelaySupport.Rec::eventId).toList()).containsExactlyElementsOf(ids);
        assertThat(unpublished(key)).isZero();
    }

    // RT5
    @Test
    void theBatchStopsAtTheFirstFailureAndLaterRowsAreNeitherSentNorMarked() {
        RelaySupport.drain(relay);
        Wallet w = wallet();
        String key = String.valueOf(w.accountId());
        deposits(w, 3);
        List<Long> ids = outboxIds(key);
        FailingSender fs = new FailingSender(sender);
        fs.failOnId = ids.get(1);
        OutboxRelay relay2 = new OutboxRelay(jdbc, fs, 100);

        OutboxRelay.BatchResult r = relay2.publishBatch();

        assertThat(r.failure()).isNotNull();
        assertThat(r.published()).isEqualTo(1);
        assertThat(r.failedRow().id()).isEqualTo(ids.get(1));
        assertThat(r.consecutiveFailures()).isEqualTo(1);
        assertThat(fs.attempted).as("row 3 was never attempted").containsExactly(ids.get(0), ids.get(1));
        assertThat(published(ids.get(0))).isTrue();
        assertThat(published(ids.get(1))).isFalse();
        assertThat(published(ids.get(2))).isFalse();
        assertThat(RelaySupport.read(bootstrap, TOPIC, Set.of(key)).stream().map(RelaySupport.Rec::eventId).toList())
                .as("only row 1 is on the topic").containsExactly(ids.get(0));

        fs.failOnId = -1;
        OutboxRelay.BatchResult r2 = relay2.publishBatch();
        assertThat(r2.failure()).isNull();
        assertThat(r2.published()).isEqualTo(2);
        assertThat(RelaySupport.read(bootstrap, TOPIC, Set.of(key)).stream().map(RelaySupport.Rec::eventId).toList())
                .as("after recovery, in order").containsExactlyElementsOf(ids);
        assertThat(unpublished(key)).isZero();
    }

    // RT6
    @Test
    void aFailureBetweenPublishAndMarkGivesADuplicateNeverALoss() {
        RelaySupport.drain(relay);
        Wallet w = wallet();
        String key = String.valueOf(w.accountId());
        deposits(w, 1);
        long id = outboxIds(key).get(0);

        installMarkFailingTrigger(key);
        try {
            OutboxRelay.BatchResult r = relay.publishBatch();
            assertThat(r.failure()).isNotNull();
            assertThat(r.published()).isZero();
            assertThat(r.failedRow().id()).isEqualTo(id);
            assertThat(RelaySupport.read(bootstrap, TOPIC, Set.of(key))).as("the record is on the topic").hasSize(1);
            assertThat(published(id)).as("but the row is still unpublished").isFalse();
        } finally {
            dropMarkFailingTrigger();
        }

        OutboxRelay.BatchResult r2 = relay.publishBatch();
        assertThat(r2.failure()).isNull();
        assertThat(r2.published()).isEqualTo(1);
        List<RelaySupport.Rec> recs = RelaySupport.read(bootstrap, TOPIC, Set.of(key));
        assertThat(recs).hasSize(2);
        assertThat(recs).allSatisfy(x -> assertThat(x.eventId()).isEqualTo(id));
        assertThat(published(id)).isTrue();
        log.info("[RELAY RT6] duplicate shape: event-id {} appears {} times on the topic, row now published", id, recs.size());
    }

    // RT7
    @Test
    void aLateCommittedLowerIdIsNotSkipped() throws Exception {
        RelaySupport.drain(relay);
        String low = "rt7-low-" + UUID.randomUUID();
        String high = "rt7-high-" + UUID.randomUUID();
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            long lowId = insertReturningId(c, low); // not committed yet
            long highId = jdbc.sql("INSERT INTO outbox_events (aggregate_id, type, payload) "
                            + "VALUES (?, 'TransactionCompleted.v1', CAST('{}' AS jsonb)) RETURNING id")
                    .param(high).query(Long.class).single(); // committed
            assertThat(lowId).isLessThan(highId);

            OutboxRelay.BatchResult first = relay.publishBatch();
            assertThat(first.failure()).isNull();
            assertThat(first.published()).isEqualTo(1);
            assertThat(published(highId)).isTrue();
            assertThat(jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE id = ? AND published_at IS NOT NULL")
                    .param(lowId).query(Long.class).single()).as("low row is not visible or not published yet").isZero();
            assertThat(RelaySupport.read(bootstrap, TOPIC, Set.of(low))).isEmpty();
            assertThat(RelaySupport.read(bootstrap, TOPIC, Set.of(high))).hasSize(1);

            c.commit();
            OutboxRelay.BatchResult second = relay.publishBatch();
            assertThat(second.failure()).isNull();
            assertThat(second.published()).isEqualTo(1);
            assertThat(published(lowId)).as("the lower id was not skipped").isTrue();
            List<RelaySupport.Rec> recs = RelaySupport.read(bootstrap, TOPIC, Set.of(low));
            assertThat(recs).hasSize(1);
            assertThat(recs.get(0).eventId()).isEqualTo(lowId);
            assertThat(publishedAt(highId)).as("higher id was published first").isBefore(publishedAt(lowId));
        }
    }

    // RT8
    @Test
    void aMissingTopicIsRecreatedByTheRelayAndTheEventIsPublished() throws Exception {
        RelaySupport.drain(relay);
        Wallet w = wallet();
        String key = String.valueOf(w.accountId());
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            try {
                admin.deleteTopics(List.of(TOPIC)).all().get(30, TimeUnit.SECONDS);
                awaitTopicGone(admin);
                deposits(w, 1);
                long id = outboxIds(key).get(0);

                long t0 = System.nanoTime();
                OutboxRelay.BatchResult first = relay.publishBatch();
                long ms = (System.nanoTime() - t0) / 1_000_000;
                log.info("[RELAY RT8] first attempt with the topic missing: failedAfterMs={}", ms);
                assertThat(first.failure()).isNotNull();
                assertThat(first.published()).isZero();
                assertThat(ms).as("bounded").isLessThan(30_000);
                assertThat(published(id)).isFalse();

                long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
                OutboxRelay.BatchResult last = relay.publishBatch();
                int attempts = 2;
                while (last.failure() != null) {
                    if (System.nanoTime() > deadline) {
                        throw new AssertionError("relay did not recover within 90 s", last.failure());
                    }
                    last = relay.publishBatch();
                    attempts++;
                }
                log.info("[RELAY RT8] recovered on attempt {}", attempts);
                TopicDescription d = admin.describeTopics(List.of(TOPIC)).allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC);
                assertThat(d.partitions()).hasSize(3);
                assertThat(d.partitions()).allSatisfy(p -> assertThat(p.replicas()).hasSize(1));
                assertThat(RelaySupport.read(bootstrap, TOPIC, Set.of(key)).stream().map(RelaySupport.Rec::eventId).toList())
                        .containsExactly(id);
                assertThat(published(id)).isTrue();
            } finally {
                restoreTopic(admin);
            }
        }
    }

    // RT9
    @Test
    void brokerAutoCreationIsOffSoASendToAnUndeclaredTopicFails() throws Exception {
        String topic = "undeclared-" + UUID.randomUUID();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000);
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 4000);
        cfg.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        try (KafkaProducer<String, String> p = new KafkaProducer<>(cfg);
             AdminClient admin = AdminClient.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            long t0 = System.nanoTime();
            assertThatThrownBy(() -> p.send(new ProducerRecord<>(topic, "k", "v")).get(10, TimeUnit.SECONDS))
                    .isInstanceOf(Exception.class);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.info("[RELAY RT9] send to an undeclared topic failed after {} ms", ms);
            assertThat(ms).isLessThan(9_000);
            assertThat(admin.listTopics().names().get(30, TimeUnit.SECONDS)).doesNotContain(topic);
        }
    }

    private static void awaitTopicGone(AdminClient admin) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (true) {
            try {
                admin.describeTopics(List.of(TOPIC)).allTopicNames().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return;
                }
                throw e;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("topic still exists after 30 s");
            }
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
    }

    /** Leaves the shared broker with the declared topic, whatever the test did. */
    private static void restoreTopic(AdminClient admin) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            try {
                TopicDescription d = admin.describeTopics(List.of(TOPIC)).allTopicNames().get(10, TimeUnit.SECONDS).get(TOPIC);
                if (d.partitions().size() == 3) {
                    return;
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    try {
                        admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get(10, TimeUnit.SECONDS);
                    } catch (ExecutionException ignored) {
                        // still being deleted: try again until the deadline
                    }
                } else {
                    throw e;
                }
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("could not restore the topic within 60 s");
            }
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
    }

    private void installMarkFailingTrigger(String aggregateId) {
        jdbc.sql("""
                CREATE OR REPLACE FUNCTION outbox_fail_rt6() RETURNS trigger AS $$
                BEGIN
                    IF NEW.aggregate_id = '%s' THEN
                        RAISE EXCEPTION 'injected mark failure';
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
                """.formatted(aggregateId)).update();
        jdbc.sql("""
                CREATE TRIGGER outbox_fail_rt6 BEFORE UPDATE ON outbox_events
                FOR EACH ROW EXECUTE FUNCTION outbox_fail_rt6()
                """).update();
    }

    private void dropMarkFailingTrigger() {
        jdbc.sql("DROP TRIGGER IF EXISTS outbox_fail_rt6 ON outbox_events").update();
        jdbc.sql("DROP FUNCTION IF EXISTS outbox_fail_rt6()").update();
    }

    private static long insertReturningId(Connection c, String aggregateId) throws Exception {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO outbox_events (aggregate_id, type, payload) "
                + "VALUES (?, 'TransactionCompleted.v1', CAST('{}' AS jsonb)) RETURNING id")) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private void deposits(Wallet w, int n) {
        for (int i = 0; i < n; i++) {
            money.deposit(w.userId(), w.accountId(), UUID.randomUUID().toString(), 100);
        }
    }

    private List<Long> outboxIds(String key) {
        return jdbc.sql("SELECT id FROM outbox_events WHERE aggregate_id = ? ORDER BY id").param(key).query(Long.class).list();
    }

    private long unpublished(String key) {
        return jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NULL")
                .param(key).query(Long.class).single();
    }

    private boolean published(long id) {
        return jdbc.sql("SELECT published_at IS NOT NULL FROM outbox_events WHERE id = ?").param(id).query(Boolean.class).single();
    }

    private OffsetDateTime publishedAt(long id) {
        return jdbc.sql("SELECT published_at FROM outbox_events WHERE id = ?").param(id).query(OffsetDateTime.class).single();
    }

    private Wallet wallet() {
        User user = users.saveAndFlush(new User("relayfail-" + UUID.randomUUID() + "@example.test", "x"));
        Account account = accounts.saveAndFlush(new Account(user.getId(), "SGD"));
        return new Wallet(user.getId(), account.getId());
    }
}
