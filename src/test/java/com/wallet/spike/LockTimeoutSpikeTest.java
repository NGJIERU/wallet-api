package com.wallet.spike;

import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

// Spike (WP 3.0): how does a Postgres lock timeout (SQLSTATE 55P03) reach the service layer?
@TestPropertySource(properties = "logging.level.org.hibernate.SQL=DEBUG")
class LockTimeoutSpikeTest extends AbstractIntegrationTest {

    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired PlatformTransactionManager txManager;
    @Autowired EntityManager em;

    @Test
    void lockTimeoutSurfacesAs() throws Exception {
        User user = users.saveAndFlush(new User("spike-" + UUID.randomUUID() + "@example.test", "x"));
        Long id = accounts.saveAndFlush(new Account(user.getId(), "SGD")).getId();

        TransactionTemplate tx = new TransactionTemplate(txManager);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();

        // transaction 1 takes the row lock and holds it
        Future<?> holder = pool.submit(() -> tx.executeWithoutResult(status -> {
            accounts.lockAllByIdOrdered(List.of(id));
            locked.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();

        // transaction 2 asks for the same row with a 500 ms lock_timeout (transaction-local)
        Throwable thrown = catchThrowable(() -> tx.executeWithoutResult(status -> {
            em.createNativeQuery("select set_config('lock_timeout', '500ms', true)").getSingleResult();
            accounts.lockAllByIdOrdered(List.of(id));
        }));

        release.countDown();
        holder.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(thrown).isNotNull();
        String sqlState = null;
        for (Throwable t = thrown; t != null && t.getCause() != t; t = t.getCause()) {
            System.out.println("SPIKE chain: " + t.getClass().getName() + " | " + t.getMessage());
            if (t instanceof SQLException se) {
                sqlState = se.getSQLState();
                System.out.println("SPIKE sqlstate: " + sqlState);
            }
        }
        assertThat(sqlState).isEqualTo("55P03");
    }
}