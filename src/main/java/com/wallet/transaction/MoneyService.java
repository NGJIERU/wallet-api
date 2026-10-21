package com.wallet.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.account.Account;
import com.wallet.account.AccountNotFoundException;
import com.wallet.account.AccountRepository;
import com.wallet.idempotency.IdempotencyService;
import com.wallet.idempotency.StoredResponse;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class MoneyService {

    private final AccountRepository accounts;
    private final TransactionRepository transactions;
    private final LedgerEntryRepository ledger;
    private final IdempotencyService idempotency;
    private final ObjectMapper mapper;

    public MoneyService(AccountRepository accounts, TransactionRepository transactions,
                        LedgerEntryRepository ledger, IdempotencyService idempotency, ObjectMapper mapper) {
        this.accounts = accounts;
        this.transactions = transactions;
        this.ledger = ledger;
        this.idempotency = idempotency;
        this.mapper = mapper;
    }

    @Transactional
    public StoredResponse deposit(long userId, long accountId, String idempotencyKey, long amount) {
        return apply(TransactionType.DEPOSIT, userId, accountId, idempotencyKey, amount);
    }

    @Transactional
    public StoredResponse withdraw(long userId, long accountId, String idempotencyKey, long amount) {
        return apply(TransactionType.WITHDRAWAL, userId, accountId, idempotencyKey, amount);
    }

    private StoredResponse apply(TransactionType type, long userId, long accountId, String key, long amount) {
        IdempotencyService.validateKey(key);

        // someone else's account looks identical to a missing one; no key is consumed
        if (!accounts.existsByIdAndUserId(accountId, userId)) {
            throw new AccountNotFoundException();
        }

        String path = "/accounts/" + accountId + (type == TransactionType.DEPOSIT ? "/deposits" : "/withdrawals");
        String hash = IdempotencyService.hash("POST", path, "amount=" + amount);
        Optional<StoredResponse> replay = idempotency.claim(userId, key, hash);
        if (replay.isPresent()) {
            return replay.get();
        }

        Account account = accounts.lockAllByIdOrdered(List.of(accountId)).stream()
                .findFirst()
                .orElseThrow(AccountNotFoundException::new);

        Direction direction;
        if (type == TransactionType.DEPOSIT) {
            account.credit(amount);
            direction = Direction.CREDIT;
        } else {
            account.debit(amount); // throws InsufficientFundsException; the rollback also undoes the key claim
            direction = Direction.DEBIT;
        }

        Transaction txn = transactions.saveAndFlush(new Transaction(type, account.getCurrency(), amount, userId));
        ledger.save(new LedgerEntry(txn.getId(), accountId, direction, amount));

        String body = toJson(new TransactionResponse(
                txn.getId(), type, account.getCurrency(), amount, accountId, account.getBalance(), txn.getCreatedAt()));
        idempotency.complete(userId, key, 201, body);
        return new StoredResponse(201, body);
    }

    private String toJson(TransactionResponse response) {
        try {
            return mapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("could not serialize the response", e);
        }
    }
}