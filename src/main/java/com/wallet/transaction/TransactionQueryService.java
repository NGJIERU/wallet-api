package com.wallet.transaction;

import com.wallet.account.AccountNotFoundException;
import com.wallet.account.AccountRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class TransactionQueryService {

    static final int MAX_LIMIT = 100;

    private final AccountRepository accounts;
    private final LedgerEntryRepository ledger;

    public TransactionQueryService(AccountRepository accounts, LedgerEntryRepository ledger) {
        this.accounts = accounts;
        this.ledger = ledger;
    }

    @Transactional(readOnly = true)
    public TransactionHistoryPage history(long userId, long accountId, Long cursor, int limit) {
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new InvalidPageRequestException("limit must be between 1 and " + MAX_LIMIT);
        }
        if (cursor != null && cursor < 1) {
            throw new InvalidPageRequestException("cursor is not valid");
        }
        // someone else's account looks identical to a missing one
        if (!accounts.existsByIdAndUserId(accountId, userId)) {
            throw new AccountNotFoundException();
        }

        long before = cursor == null ? Long.MAX_VALUE : cursor;
        // ask for one extra row: if it comes back, there is a next page (no count query needed)
        List<TransactionHistoryItem> rows = ledger.findHistory(accountId, before, PageRequest.of(0, limit + 1));
        if (rows.size() <= limit) {
            return new TransactionHistoryPage(rows, null);
        }
        List<TransactionHistoryItem> page = List.copyOf(rows.subList(0, limit));
        return new TransactionHistoryPage(page, page.get(limit - 1).ledgerEntryId());
    }

    @Transactional(readOnly = true)
    public TransactionDetailResponse get(long userId, UUID transactionId) {
        List<TransactionDetailRow> rows = ledger.findOwnedRows(transactionId, userId);
        if (rows.isEmpty()) {
            throw new TransactionNotFoundException();
        }
        TransactionDetailRow first = rows.get(0);
        List<TransactionDetailResponse.Entry> entries = rows.stream()
                .map(r -> new TransactionDetailResponse.Entry(r.accountId(), r.direction()))
                .toList();
        return new TransactionDetailResponse(
                first.id(), first.type(), first.currency(), first.amount(), first.createdAt(), entries);
    }
}