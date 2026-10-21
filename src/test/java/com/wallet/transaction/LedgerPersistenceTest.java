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

        Transaction txn = transactions.saveAndFlush(
                new Transaction(TransactionType.TRANSFER, "SGD", 2_500, alice.getId()));
        LedgerEntry debit = ledger.saveAndFlush(new LedgerEntry(txn.getId(), from.getId(), Direction.DEBIT, 2_500));
        LedgerEntry credit = ledger.saveAndFlush(new LedgerEntry(txn.getId(), to.getId(), Direction.CREDIT, 2_500));

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