package com.wallet.notification;

import com.wallet.AbstractIntegrationTest;
import com.wallet.idempotency.StoredResponse;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.transaction.MoneyService;
import com.wallet.transaction.TransferService;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import com.wallet.config.KafkaTopicConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CT9: the one extra Spring context (relay AND consumer on). API-level services -> outbox -> relay -> Kafka ->
 * listener -> notifications. A unique consumer group per run; assertions are scoped to this test's own accounts
 * because the topic is shared by the whole suite (the new group first reads the old backlog; dedupe makes that safe).
 */
@TestPropertySource(properties = {
        "app.outbox.relay.enabled=true",
        "app.outbox.relay.poll-interval=100ms",
        "app.notifications.consumer.enabled=true",
        "app.notifications.consumer-group=chain-${random.uuid}",
        "app.notifications.retry.initial-backoff=50ms",
        "app.lock-timeout-ms=30000"})
class NotificationFullChainTest extends AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(NotificationFullChainTest.class);

    @Autowired MoneyService money;
    @Autowired TransferService transfers;
    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired JdbcClient jdbc;
    @Value("${spring.kafka.bootstrap-servers}") String bootstrap;

    private record Wallet(long userId, long accountId) {
    }

    @Test
    void depositsTransfersAndARandomWorkloadEndUpAsExactlyOneNotificationPerLedgerEntryInOrder() throws Exception {
        Wallet a = wallet();
        Wallet b = wallet();
        long t0 = System.nanoTime();
        money.deposit(a.userId(), a.accountId(), key(), 10_000);
        transfers.transfer(a.userId(), key(), a.accountId(), b.accountId(), 2_500);
        awaitNotifications(List.of(a, b), 3, 90);
        assertThat(count("SELECT COUNT(*) FROM notifications WHERE user_id = ?", a.userId())).isEqualTo(2); // credit + debit
        assertThat(count("SELECT COUNT(*) FROM notifications WHERE user_id = ?", b.userId())).isEqualTo(1);
        assertThat(jdbc.sql("SELECT message FROM notifications WHERE user_id = ? AND direction = 'CREDIT'")
                .param(b.userId()).query(String.class).single()).isEqualTo("Received SGD 25.00 (transfer)");
        log.info("[CHAIN CT9] first notifications after {} ms (includes reading the old topic backlog)", (System.nanoTime() - t0) / 1_000_000);

        List<Wallet> ws = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            Wallet w = wallet();
            money.deposit(w.userId(), w.accountId(), key(), 10_000);
            ws.add(w);
        }
        Random rnd = new Random(42);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                long seed = rnd.nextLong();
                fs.add(pool.submit(() -> {
                    Random r = new Random(seed);
                    for (int i = 0; i < 25; i++) {
                        int from = r.nextInt(8);
                        int to = (from + 1 + r.nextInt(7)) % 8;
                        StoredResponse resp = transfers.transfer(ws.get(from).userId(), key(), ws.get(from).accountId(),
                                ws.get(to).accountId(), 1 + r.nextInt(50));
                        if (resp.code() != 201) throw new IllegalStateException("code " + resp.code());
                    }
                }));
            }
            for (Future<?> f : fs) f.get(60, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        long expected = 8 + 2L * 4 * 25;
        awaitNotifications(ws, expected, 90);

        for (Wallet w : ws) {
            List<Long> fromLedger = jdbc.sql("SELECT id FROM ledger_entries WHERE account_id = ? ORDER BY id")
                    .param(w.accountId()).query(Long.class).list();
            List<Long> fromNotifications = jdbc.sql("SELECT ledger_entry_id FROM notifications WHERE account_id = ? ORDER BY id")
                    .param(w.accountId()).query(Long.class).list();
            assertThat(fromNotifications).as("ledger entry ids in notification order, account " + w.accountId()).isEqualTo(fromLedger);
        }
        assertThat(jdbc.sql("SELECT COUNT(*) FROM (SELECT ledger_entry_id FROM notifications WHERE account_id IN (:ids) "
                        + "GROUP BY ledger_entry_id HAVING COUNT(*) > 1) d")
                .param("ids", ws.stream().map(Wallet::accountId).toList()).query(Long.class).single()).as("duplicates").isZero();
        log.info("[CHAIN CT9] workload notifications={} expected={} perAccountOrderOk=true", expected, expected);
    }

    private static final String MAIN = KafkaTopicConfig.TRANSACTION_COMPLETED_TOPIC;
    private static final String DLT = MAIN + "-dlt";

    // DLT1: a poison record is dead-lettered with its original data and the consumer is not blocked
    @Test
    void aPoisonRecordIsDeadLetteredAndTheNextValidEventStillProducesItsNotification() throws Exception {
        Wallet w = wallet();
        String key = "dlt1-" + UUID.randomUUID();
        long eventId = newEventId();
        produce(key, null, eventId, "not json {");
        money.deposit(w.userId(), w.accountId(), key(), 500);

        awaitNotifications(List.of(w), 1, 60);
        List<ConsumerRecord<String, String>> dead = awaitDlt(key, 1);
        assertThat(dead).hasSize(1);
        ConsumerRecord<String, String> r = dead.get(0);
        assertThat(r.key()).isEqualTo(key);
        assertThat(r.value()).isEqualTo("not json {");
        assertThat(header(r, "event-id")).isEqualTo(Long.toString(eventId));
        assertThat(header(r, "event-type")).isEqualTo("TransactionCompleted.v1");
        assertThat(header(r, "kafka_dlt-original-topic")).isEqualTo(MAIN);
        assertThat((header(r, "kafka_dlt-exception-fqcn") + " " + header(r, "kafka_dlt-exception-cause-fqcn")))
                .contains("PoisonEventException");
        assertThat(count("SELECT COUNT(*) FROM notifications WHERE event_id = ?", eventId)).isZero();
    }

    // DLT2: a retryable failure that never clears ends on the DLT after the configured attempts; the next record is processed
    @Test
    void aRetryableFailureThatNeverClearsIsDeadLetteredAndTheConsumerContinues() throws Exception {
        Wallet failing = wallet();
        Wallet fine = wallet();
        String failKey = "dlt2-fail-" + UUID.randomUUID();
        String okKey = "dlt2-ok-" + UUID.randomUUID();
        long failId = newEventId();
        long okId = newEventId();
        jdbc.sql("""
                CREATE OR REPLACE FUNCTION notif_fail_dlt2() RETURNS trigger AS $$
                BEGIN
                    IF NEW.user_id = %d THEN RAISE EXCEPTION 'injected notification failure'; END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
                """.formatted(failing.userId())).update();
        jdbc.sql("CREATE TRIGGER notif_fail_dlt2 BEFORE INSERT ON notifications FOR EACH ROW EXECUTE FUNCTION notif_fail_dlt2()").update();
        try {
            produce(failKey, 0, failId, NotificationTestData.payload(UUID.randomUUID(), 1L, failing.accountId(), failing.userId(), "CREDIT", "DEPOSIT", 100, "SGD"));
            produce(okKey, 0, okId, NotificationTestData.payload(UUID.randomUUID(), 2L, fine.accountId(), fine.userId(), "CREDIT", "DEPOSIT", 100, "SGD"));

            List<ConsumerRecord<String, String>> dead = awaitDlt(failKey, 1);
            assertThat(dead).hasSize(1);
            assertThat(header(dead.get(0), "event-id")).isEqualTo(Long.toString(failId));
            await(() -> count("SELECT COUNT(*) FROM notifications WHERE event_id = ?", okId) == 1, 60, "the next record was not processed");
        } finally {
            jdbc.sql("DROP TRIGGER IF EXISTS notif_fail_dlt2 ON notifications").update();
            jdbc.sql("DROP FUNCTION IF EXISTS notif_fail_dlt2()").update();
        }
        assertThat(count("SELECT COUNT(*) FROM notifications WHERE event_id = ?", failId)).isZero();
        assertThat(count("SELECT COUNT(*) FROM processed_events WHERE event_id = ?", failId)).as("marker rolled back").isZero();
    }

    // DLT3: the DLT has the same partition count; a record from every main partition number can be dead-lettered
    @Test
    void theDeadLetterTopicHasTheSamePartitionsAndEveryMainPartitionCanBeDeadLettered() throws Exception {
        String run = "dlt3-" + UUID.randomUUID() + "-";
        for (int p = 0; p < 3; p++) {
            produce(run + p, p, newEventId(), "bad " + p);
        }
        List<ConsumerRecord<String, String>> dead = awaitDlt(run, 3);
        assertThat(dead).hasSize(3);
        Set<Integer> partitions = new HashSet<>();
        for (ConsumerRecord<String, String> r : dead) {
            int original = ByteBuffer.wrap(r.headers().lastHeader("kafka_dlt-original-partition").value()).getInt();
            assertThat(r.partition()).as("same partition number as the original").isEqualTo(original);
            partitions.add(r.partition());
        }
        assertThat(partitions).containsExactlyInAnyOrder(0, 1, 2);
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            TopicDescription d = admin.describeTopics(List.of(DLT)).allTopicNames().get(30, TimeUnit.SECONDS).get(DLT);
            assertThat(d.partitions()).hasSize(3);
        }
    }

    private static long newEventId() {
        return ThreadLocalRandom.current().nextLong(1L << 40, 1L << 50);
    }

    private static String header(ConsumerRecord<?, ?> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null || h.value() == null ? "" : new String(h.value(), StandardCharsets.UTF_8);
    }

    private void produce(String key, Integer partition, long eventId, String payload) throws Exception {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            ProducerRecord<String, String> r = new ProducerRecord<>(MAIN, partition, key, payload);
            r.headers().add("event-id", Long.toString(eventId).getBytes(StandardCharsets.UTF_8));
            r.headers().add("event-type", "TransactionCompleted.v1".getBytes(StandardCharsets.UTF_8));
            p.send(r).get(30, TimeUnit.SECONDS);
        }
    }

    private List<ConsumerRecord<String, String>> awaitDlt(String keyPrefix, int expected) {
        await(() -> readTopic(DLT, keyPrefix).size() >= expected, 60, "dead-letter records did not reach " + expected);
        return readTopic(DLT, keyPrefix);
    }

    /** Reads a topic from the beginning up to the end offsets taken at the start, keeping keys with the prefix. */
    private List<ConsumerRecord<String, String>> readTopic(String topic, String keyPrefix) {
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            List<TopicPartition> tps = c.partitionsFor(topic).stream().map(p -> new TopicPartition(topic, p.partition())).toList();
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (tps.stream().anyMatch(tp -> c.position(tp) < end.get(tp))) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("did not reach the end offsets of " + topic);
                }
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(200))) {
                    if (r.key() != null && r.key().startsWith(keyPrefix)) {
                        out.add(r);
                    }
                }
            }
        }
        return out;
    }

    private void awaitNotifications(List<Wallet> ws, long expected, int seconds) {
        List<Long> ids = ws.stream().map(Wallet::accountId).toList();
        await(() -> jdbc.sql("SELECT COUNT(*) FROM notifications WHERE account_id IN (:ids)")
                .param("ids", ids).query(Long.class).single() >= expected, seconds,
                "notifications did not reach " + expected);
    }

    private static void await(BooleanSupplier cond, int seconds, String message) {
        long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError(message + " within " + seconds + " s");
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
    }

    private long count(String sql, Object p) {
        return jdbc.sql(sql).param(p).query(Long.class).single();
    }

    private Wallet wallet() {
        User u = users.saveAndFlush(new User("chain-" + UUID.randomUUID() + "@example.test", "x"));
        Account acc = accounts.saveAndFlush(new Account(u.getId(), "SGD"));
        return new Wallet(u.getId(), acc.getId());
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
