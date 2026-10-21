package com.wallet.transaction;

import com.wallet.AbstractIntegrationTest;
import com.wallet.account.Account;
import com.wallet.account.AccountRepository;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class LedgerPersistenceTest extends AbstractIntegrationTest {

    @Autowired UserRepository users;
    @Autowired AccountRepository accounts;
    @Autowired TransactionRepository transactions;
    @Autowired LedgerEntryRepository ledger;

    @Test
    void transferWithTwoLedgerEntriesRoundTrips() {
        User alice = users.saveAndFlush(new User("alice-" + UUID.randomUUID() + "@example.test", "x"));
        User bob = users.saveAndFlush(new User("bob-" + UUID.randomUUID() + "@example.test", "x"));
        Account from = accounts.saveAndFlush(new Account(alice.getId(), "SGD"));
        Account to = accounts.saveAndFlush(new Account(bob.getId(), "SGD"));

        // fund the sender first so the balances and the ledger stay consistent (reconciliation runs after every test)
        Transaction funding = transactions.saveAndFlush(
                new Transaction(TransactionType.DEPOSIT, "SGD", 2_500, alice.getId()));
        ledger.saveAndFlush(new LedgerEntry(funding.getId(), from.getId(), Direction.CREDIT, 2_500));
        from.credit(2_500);

        Transaction txn = transactions.saveAndFlush(
                new Transaction(TransactionType.TRANSFER, "SGD", 2_500, alice.getId()));
        LedgerEntry debit = ledger.saveAndFlush(new LedgerEntry(txn.getId(), from.getId(), Direction.DEBIT, 2_500));
        LedgerEntry credit = ledger.saveAndFlush(new LedgerEntry(txn.getId(), to.getId(), Direction.CREDIT, 2_500));
        from.debit(2_500);
        to.credit(2_500);
        accounts.saveAndFlush(from);
        accounts.saveAndFlush(to);

        assertThat(txn.getId()).isNotNull();

        Transaction loaded = transactions.findById(txn.getId()).orElseThrow();
        assertThat(loaded.getType()).isEqualTo(TransactionType.TRANSFER);
        assertThat(loaded.getAmount()).isEqualTo(2_500);
        assertThat(loaded.getCreatedBy()).isEqualTo(alice.getId());

        LedgerEntry loadedDebit = ledger.findById(debit.getId()).orElseThrow();
        assertThat(loadedDebit.getDirection()).isEqualTo(Direction.DEBIT);
        assertThat(loadedDebit.getAccountId()).isEqualTo(from.getId());
        assertThat(loadedDebit.getTransactionId()).isEqualTo(txn.getId());

        assertThat(ledger.findById(credit.getId()).orElseThrow().getDirection()).isEqualTo(Direction.CREDIT);
    }
}