package com.wallet;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/** Fails any integration test that leaves an account whose balance differs from its ledger. */
public class ReconciliationExtension implements AfterEachCallback {

    @Override
    public void afterEach(ExtensionContext context) {
        JdbcClient jdbc = SpringExtension.getApplicationContext(context).getBean(JdbcClient.class);
        Reconciliation.assertAllReconciled(jdbc, context.getDisplayName());
    }
}