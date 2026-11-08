package com.wallet.concurrency;

import com.wallet.notification.NotificationHandler;
import com.wallet.notification.NotificationTestData;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** CT5: 20 threads handle the same event at once. */
class NotificationConcurrencyTest extends ConcurrencyTestBase {

    @Autowired NotificationHandler handler;
    @Value("${app.notifications.consumer-group:wallet-notifications}") String group;

    @Test
    void twentyThreadsHandlingTheSameEventCreateExactlyOneNotification() {
        Wallet w = wallet("SGD");
        long eventId = ThreadLocalRandom.current().nextLong(1L << 40, 1L << 50);
        String payload = NotificationTestData.payload(UUID.randomUUID(), 1L, w.accountId(), w.userId(), "CREDIT", "DEPOSIT", 100, "SGD");

        List<Callable<NotificationHandler.Result>> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            tasks.add(() -> handler.handle(Long.toString(eventId), "TransactionCompleted.v1", payload));
        }
        ConcurrentRunner.RunResult<NotificationHandler.Result> r = ConcurrentRunner.run(tasks);
        r.log("notification-duplicate-race");

        r.assertNoUnexpected();
        List<NotificationHandler.Result> results = r.okValues();
        assertThat(results).hasSize(20);
        assertThat(results.stream().filter(x -> x == NotificationHandler.Result.CREATED).count()).isEqualTo(1);
        assertThat(results.stream().filter(x -> x == NotificationHandler.Result.DUPLICATE).count()).isEqualTo(19);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM notifications WHERE event_id = ?").param(eventId).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM processed_events WHERE consumer_group = ? AND event_id = ?")
                .param(group).param(eventId).query(Long.class).single()).isEqualTo(1);
    }
}
