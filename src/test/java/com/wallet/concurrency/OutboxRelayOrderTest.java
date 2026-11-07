package com.wallet.concurrency;

import com.wallet.config.KafkaTopicConfig;
import com.wallet.idempotency.StoredResponse;
import com.wallet.outbox.OutboxRelay;
import com.wallet.outbox.RelaySupport;
import com.wallet.transaction.TransferService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

/** RT3: the end-to-end ordering and completeness proof (T9-style workload, then drain, then read the topic). */
class OutboxRelayOrderTest extends ConcurrencyTestBase {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayOrderTest.class);
    private static final int ACCOUNTS = 8;
    private static final int THREADS = 16;
    private static final int PER_THREAD = 25;

    @Autowired TransferService transfers;
    @Autowired OutboxRelay relay;
    @Autowired ObjectMapper mapper;
    @Value("${spring.kafka.bootstrap-servers}") String bootstrap;

    private record Plan(int from, int to, long amount) {
    }

    @Test
    void everyLedgerEntryAppearsOnTheTopicExactlyOnceInPerAccountOrder() {
        List<Wallet> wallets = new ArrayList<>();
        for (int i = 0; i < ACCOUNTS; i++) {
            Wallet w = wallet("SGD");
            fund(w, 10_000);
            wallets.add(w);
        }
        List<String> keys = wallets.stream().map(w -> String.valueOf(w.accountId())).toList();

        Random rnd = random();
        List<Callable<Integer>> tasks = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            List<Plan> plans = new ArrayList<>();
            for (int i = 0; i < PER_THREAD; i++) {
                int from = rnd.nextInt(ACCOUNTS);
                int to = (from + 1 + rnd.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
                plans.add(new Plan(from, to, 1 + rnd.nextInt(50)));
            }
            tasks.add(() -> {
                for (Plan p : plans) {
                    Wallet src = wallets.get(p.from());
                    Wallet dst = wallets.get(p.to());
                    StoredResponse resp = transfers.transfer(src.userId(), key(), src.accountId(), dst.accountId(), p.amount());
                    if (resp.code() != 201) {
                        throw new IllegalStateException("unexpected response code " + resp.code());
                    }
                }
                return plans.size();
            });
        }
        ConcurrentRunner.RunResult<Integer> r = ConcurrentRunner.run(tasks);
        r.log("relay-order-workload");
        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(THREADS);

        long t0 = System.nanoTime();
        int publishedTotal = RelaySupport.drain(relay);
        long drainMs = (System.nanoTime() - t0) / 1_000_000;

        long ledgerTotal = ledgerRows(wallets);
        List<RelaySupport.Rec> recs = RelaySupport.read(bootstrap, KafkaTopicConfig.TRANSACTION_COMPLETED_TOPIC, keys);
        assertThat(ledgerTotal).isEqualTo(ACCOUNTS + 2L * THREADS * PER_THREAD);
        assertThat(recs).as("records on the topic for these accounts").hasSize((int) ledgerTotal);
        assertThat(recs.stream().map(RelaySupport.Rec::eventId).distinct().count()).as("unique event ids").isEqualTo(recs.size());

        for (Wallet w : wallets) {
            String k = String.valueOf(w.accountId());
            List<RelaySupport.Rec> mine = recs.stream().filter(x -> x.key().equals(k)).toList();
            List<Long> fromTopic = mine.stream()
                    .map(x -> mapper.readTree(x.value()).get("ledgerEntryId").asLong()).toList();
            List<Long> fromLedger = jdbc.sql("SELECT id FROM ledger_entries WHERE account_id = ? ORDER BY id")
                    .param(w.accountId()).query(Long.class).list();
            assertThat(fromTopic).as("ledger entry ids in topic order, account " + k).isEqualTo(fromLedger);
            assertThat(mine.stream().map(RelaySupport.Rec::partition).distinct().count())
                    .as("one partition per key, account " + k).isEqualTo(1);
        }
        long unpublished = jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id IN (:k) AND published_at IS NULL")
                .param("k", keys).query(Long.class).single();
        assertThat(unpublished).isZero();
        log.info("[RELAY RT3] ledger={} records={} perAccountOrderAndCompletenessOk=true drained={} drainMs={}",
                ledgerTotal, recs.size(), publishedTotal, drainMs);
    }
}
