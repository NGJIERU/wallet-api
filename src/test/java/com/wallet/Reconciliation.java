package com.wallet;

import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** SUM(CREDIT) - SUM(DEBIT) must equal accounts.balance. Scoped to the given accounts, not the whole table. */
public final class Reconciliation {

    private Reconciliation() {
    }

    public static void assertReconciled(JdbcClient jdbc, long... accountIds) {
        List<Long> ids = Arrays.stream(accountIds).boxed().toList();
        List<String> broken = jdbc.sql("""
                        SELECT 'account ' || a.id || ' balance=' || a.balance || ' ledger='
                               || COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0)
                        FROM accounts a LEFT JOIN ledger_entries l ON l.account_id = a.id
                        WHERE a.id IN (:ids)
                        GROUP BY a.id, a.balance
                        HAVING a.balance <> COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0)
                        """)
                .param("ids", ids)
                .query(String.class)
                .list();
        assertThat(broken).as("accounts whose balance differs from their ledger").isEmpty();
    }

    /** Same invariant over every account in the database. Run by ReconciliationExtension after each test. */
    public static void assertAllReconciled(JdbcClient jdbc, String afterTest) {
        List<String> broken = jdbc.sql("""
                        SELECT 'account ' || a.id || ' balance=' || a.balance || ' ledger='
                               || COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0)
                        FROM accounts a LEFT JOIN ledger_entries l ON l.account_id = a.id
                        GROUP BY a.id, a.balance
                        HAVING a.balance <> COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0)
                        ORDER BY a.id
                        LIMIT 20
                        """)
                .query(String.class)
                .list();
        assertThat(broken).as("reconciliation after " + afterTest).isEmpty();
    }
}