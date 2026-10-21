package com.wallet.transaction;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, Long> {

    // One query for the whole page: no lazy loading, so the statement count does not grow with the page size.
    // Uses idx_ledger_account_id_id. The Pageable only supplies LIMIT; the ORDER BY lives in the query.
    @Query("""
            select new com.wallet.transaction.TransactionHistoryItem(
                l.id, t.id, t.type, l.direction, l.amount, t.currency, t.createdAt)
            from LedgerEntry l join Transaction t on t.id = l.transactionId
            where l.accountId = :accountId and l.id < :before
            order by l.id desc
            """)
    List<TransactionHistoryItem> findHistory(@Param("accountId") Long accountId,
                                             @Param("before") Long before,
                                             Pageable pageable);

    // Only the entries on accounts the user owns. Empty means "not found" (also for someone else's transaction).
    @Query("""
            select new com.wallet.transaction.TransactionDetailRow(
                t.id, t.type, t.currency, t.amount, t.createdAt, l.accountId, l.direction)
            from LedgerEntry l
            join Transaction t on t.id = l.transactionId
            join Account a on a.id = l.accountId
            where t.id = :transactionId and a.userId = :userId
            order by l.id
            """)
    List<TransactionDetailRow> findOwnedRows(@Param("transactionId") UUID transactionId,
                                             @Param("userId") Long userId);
}