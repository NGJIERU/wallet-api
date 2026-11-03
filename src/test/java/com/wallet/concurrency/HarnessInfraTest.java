package com.wallet.concurrency;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

class HarnessInfraTest extends ConcurrencyTestBase {

    @Autowired DataSource dataSource;
    @Value("${app.lock-timeout-ms}") long lockTimeoutMs;

    @Test
    void heavySettingsAreApplied() throws Exception {
        assertThat(dataSource.unwrap(HikariDataSource.class).getMaximumPoolSize()).isEqualTo(50);
        assertThat(lockTimeoutMs).isEqualTo(30_000);
        String max = jdbc.sql("SHOW max_connections").query(String.class).single();
        assertThat(max).isEqualTo("200");
    }

    @Test
    void twentyThreadsFundTheirOwnWalletsThroughTheRunner() {
        List<Wallet> wallets = new ArrayList<>();
        List<Callable<Long>> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Wallet w = wallet("SGD");
            wallets.add(w);
            tasks.add(() -> { fund(w, 500); return balance(w); });
        }

        ConcurrentRunner.RunResult<Long> r = ConcurrentRunner.run(tasks);
        r.log("infra");

        r.assertNoUnexpected();
        assertThat(r.count(Outcome.OK)).isEqualTo(20);
        assertThat(totalBalance(wallets)).isEqualTo(10_000);
        assertThat(ledgerRows(wallets)).isEqualTo(20);
        assertNoNegativeBalance(wallets);
    }
}