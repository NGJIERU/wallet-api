package com.wallet.outbox;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Test helpers: drain the relay, and read the shared topic filtered by key (no sleeps, bounded by deadlines). */
public final class RelaySupport {

    public record Rec(String key, String value, long eventId, String eventType, int partition, long offset) {
    }

    private RelaySupport() {
    }

    /** Runs batches until nothing is left. Publishes EVERY unpublished row in the shared DB, not only the test's. */
    public static int drain(OutboxRelay relay) {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        int total = 0;
        while (true) {
            OutboxRelay.BatchResult r = relay.publishBatch();
            if (r.failure() != null) {
                throw new AssertionError("relay failed at row " + r.failedRow(), r.failure());
            }
            total += r.published();
            if (r.selected() == 0) {
                return total;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("drain did not finish within 120 s");
            }
        }
    }

    /** Reads the topic from the beginning up to the end offsets taken at the start; keeps records for the keys. */
    public static List<Rec> read(String bootstrap, String topic, Collection<String> keys) {
        Set<String> wanted = new HashSet<>(keys);
        List<Rec> out = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            List<TopicPartition> tps = c.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (!reachedEnd(c, tps, end)) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("did not reach the end offsets within 60 s");
                }
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(200))) {
                    if (wanted.contains(r.key())) {
                        out.add(toRec(r));
                    }
                }
            }
        }
        return out;
    }

    private static boolean reachedEnd(KafkaConsumer<String, String> c, List<TopicPartition> tps,
                                      Map<TopicPartition, Long> end) {
        for (TopicPartition tp : tps) {
            if (c.position(tp) < end.get(tp)) {
                return false;
            }
        }
        return true;
    }

    private static Rec toRec(ConsumerRecord<String, String> r) {
        return new Rec(r.key(), r.value(), Long.parseLong(header(r, "event-id")), header(r, "event-type"),
                r.partition(), r.offset());
    }

    private static String header(ConsumerRecord<String, String> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
