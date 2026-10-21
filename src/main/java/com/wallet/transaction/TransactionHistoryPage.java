package com.wallet.transaction;

import java.util.List;

/** nextCursor is null on the last page. Pass it back as ?cursor= to get the next (older) page. */
public record TransactionHistoryPage(List<TransactionHistoryItem> items, Long nextCursor) {
}