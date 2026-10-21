package com.wallet.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.account.Account;
import com.wallet.account.AccountNotFoundException;
import com.wallet.account.AccountRepository;
import com.wallet.account.DestinationAccountNotFoundException;
import com.wallet.idempotency.IdempotencyService;
import com.wallet.idempotency.StoredResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** A single attempt at a transfer. Retrying is done by TransferService, outside this transaction. */
@Service
public class TransferTransaction {

    private static final Logger log = LoggerFactory.getLogger(TransferTransaction.class);

    private final AccountRepository accounts;
    private final TransactionRepository transactions;
    private final LedgerEntryRepository ledger;
    private final IdempotencyService idempotency;
    private final ObjectMapper mapper;

    public TransferTransaction(AccountRepository accounts, TransactionRepository transactions,
                               LedgerEntryRepository ledger, IdempotencyService idempotency, ObjectMapper mapper) {
        this.accounts = accounts;
        this.transactions = transactions;
        this.ledger = ledger;
        this.idempotency = idempotency;
        this.mapper = mapper;
    }

    @Transactional
    public StoredResponse execute(long userId, String key, long fromId, long toId, long amount) {
        IdempotencyService.validateKey(key);

        // the source must be yours; someone else's account looks identical to a missing one
        if (!accounts.existsByIdAndUserId(fromId, userId)) {
            throw new AccountNotFoundException();
        }
        if (fromId == toId) {
            throw new InvalidTransferException("Cannot transfer to the same account");
        }

        String hash = IdempotencyService.hash("POST", "/transfers",
                "from=" + fromId + "&to=" + toId + "&amount=" + amount);
        Optional<StoredResponse> replay = idempotency.claim(userId, key, hash);
        if (replay.isPresent()) {
            return replay.get();
        }

        // both rows locked in id order (ORDER BY id), so two opposite transfers cannot deadlock
        Map<Long, Account> locked = accounts.lockAllByIdOrdered(List.of(fromId, toId)).stream()
                .collect(Collectors.toMap(Account::getId, a -> a));
        Account from = locked.get(fromId);
        Account to = locked.get(toId);
        if (from == null) {
            throw new AccountNotFoundException();
        }
        if (to == null) {
            // DECISION: transfers to other users' accounts are allowed (Option 1), and a missing
            // destination gets its own message so it is easy to debug. The cost is that this
            // reveals which account ids exist. To hide that, throw AccountNotFoundException here.
            log.debug("transfer rejected: destination account {} not found (user={}, from={})", toId, userId, fromId);
            throw new DestinationAccountNotFoundException(toId);
        }
        if (!from.getCurrency().equals(to.getCurrency())) {
            throw new InvalidTransferException("Currency mismatch: " + from.getCurrency() + " to " + to.getCurrency());
        }

        from.debit(amount); // throws InsufficientFundsException; the rollback also undoes the key claim
        to.credit(amount);

        Transaction txn = transactions.saveAndFlush(
                new Transaction(TransactionType.TRANSFER, from.getCurrency(), amount, userId));
        ledger.save(new LedgerEntry(txn.getId(), fromId, Direction.DEBIT, amount));
        ledger.save(new LedgerEntry(txn.getId(), toId, Direction.CREDIT, amount));

        String body = toJson(new TransferResponse(
                txn.getId(), TransactionType.TRANSFER, from.getCurrency(), amount,
                fromId, toId, from.getBalance(), txn.getCreatedAt()));
        idempotency.complete(userId, key, 201, body);
        return new StoredResponse(201, body);
    }

    private String toJson(TransferResponse response) {
        try {
            return mapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("could not serialize the response", e);
        }
    }
}