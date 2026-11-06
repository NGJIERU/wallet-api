package com.wallet.outbox;

import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.transaction.MoneyService;
import com.wallet.transaction.TransferService;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.UUID;

import static com.wallet.Reconciliation.assertReconciled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** T7: if the outbox insert fails, the whole money movement rolls back. Fault injected with a DB trigger. */
class OutboxFailureTest extends AbstractIntegrationTest {

    @Autowired MoneyService money;
    @Autowired TransferService transfers;
    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired JdbcClient jdbc;

    private record Wallet(long userId, long accountId) {
    }

    private record Snapshot(long aliceBalance, long victimBalance, long aliceLedger, long victimLedger,
                            long aliceEvents, long victimEvents) {
    }

    @Test
    void aFailingOutboxInsertRollsBackTheWholeMoneyMovement() {
        Wallet alice = wallet();
        Wallet victim = wallet();
        money.deposit(alice.userId(), alice.accountId(), key(), 1_000);
        money.deposit(victim.userId(), victim.accountId(), key(), 1_000);
        Snapshot before = snapshot(alice, victim);
        assertThat(before.aliceEvents()).isEqualTo(1);
        assertThat(before.victimEvents()).isEqualTo(1);

        String depositKey = key();
        String withdrawKey = key();
        String transferKey = key();

        installFailingTrigger(victim.accountId());
        try {
            // deposit into the victim account
            assertInjectedFailure(() -> money.deposit(victim.userId(), victim.accountId(), depositKey, 100));
            assertThat(snapshot(alice, victim)).isEqualTo(before);
            assertThat(keyRows(victim.userId(), depositKey)).isZero();

            // withdrawal from the victim account
            assertInjectedFailure(() -> money.withdraw(victim.userId(), victim.accountId(), withdrawKey, 100));
            assertThat(snapshot(alice, victim)).isEqualTo(before);
            assertThat(keyRows(victim.userId(), withdrawKey)).isZero();

            // transfer alice -> victim: alice's DEBIT event is written first, then the victim's CREDIT insert fails,
            // so the rollback must remove alice's debit, ledger row, balance change and key too
            assertInjectedFailure(() -> transfers.transfer(
                    alice.userId(), transferKey, alice.accountId(), victim.accountId(), 100));
            assertThat(snapshot(alice, victim)).isEqualTo(before);
            assertThat(keyRows(alice.userId(), transferKey)).isZero();
        } finally {
            dropFailingTrigger();
        }

        // keys were never consumed, so the same keys now succeed for real
        money.deposit(victim.userId(), victim.accountId(), depositKey, 100);
        assertThat(eventCount(victim)).isEqualTo(2);
        assertThat(balance(victim)).isEqualTo(1_100);
        assertReconciled(jdbc, alice.accountId(), victim.accountId());
    }

    private void assertInjectedFailure(ThrowingCallable call) {
        assertThatThrownBy(call).hasStackTraceContaining("injected outbox failure");
    }

    private void installFailingTrigger(long accountId) {
        jdbc.sql("""
                CREATE OR REPLACE FUNCTION outbox_fail_t7() RETURNS trigger AS $$
                BEGIN
                    IF NEW.aggregate_id = '%d' THEN
                        RAISE EXCEPTION 'injected outbox failure';
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
                """.formatted(accountId)).update();
        jdbc.sql("""
                CREATE TRIGGER outbox_fail_t7 BEFORE INSERT ON outbox_events
                FOR EACH ROW EXECUTE FUNCTION outbox_fail_t7()
                """).update();
    }

    private void dropFailingTrigger() {
        jdbc.sql("DROP TRIGGER IF EXISTS outbox_fail_t7 ON outbox_events").update();
        jdbc.sql("DROP FUNCTION IF EXISTS outbox_fail_t7()").update();
    }

    private Snapshot snapshot(Wallet alice, Wallet victim) {
        return new Snapshot(balance(alice), balance(victim), ledgerRows(alice), ledgerRows(victim),
                eventCount(alice), eventCount(victim));
    }

    private Wallet wallet() {
        User user = users.saveAndFlush(new User("outbox-fail-" + UUID.randomUUID() + "@example.test", "x"));
        Account account = accounts.saveAndFlush(new Account(user.getId(), "SGD"));
        return new Wallet(user.getId(), account.getId());
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }

    private long balance(Wallet w) {
        return jdbc.sql("SELECT balance FROM accounts WHERE id = ?").param(w.accountId()).query(Long.class).single();
    }

    private long ledgerRows(Wallet w) {
        return jdbc.sql("SELECT COUNT(*) FROM ledger_entries WHERE account_id = ?")
                .param(w.accountId()).query(Long.class).single();
    }

    private long eventCount(Wallet w) {
        return jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ?")
                .param(String.valueOf(w.accountId())).query(Long.class).single();
    }

    private long keyRows(long userId, String key) {
        return jdbc.sql("SELECT COUNT(*) FROM idempotency_keys WHERE user_id = ? AND key = ?")
                .param(userId).param(key).query(Long.class).single();
    }
}
