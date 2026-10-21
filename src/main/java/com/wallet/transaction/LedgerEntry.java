package com.wallet.transaction;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Append-only: no setters, every column updatable = false, and the application never deletes rows.
 * Holds plain id columns instead of @ManyToOne on purpose: no lazy loading, so no hidden N+1 queries.
 */
@Entity
@Table(name = "ledger_entries")
public class LedgerEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "transaction_id", nullable = false, updatable = false)
    private UUID transactionId;

    @Column(name = "account_id", nullable = false, updatable = false)
    private Long accountId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, updatable = false, length = 6)
    private Direction direction;

    @Column(nullable = false, updatable = false)
    private long amount;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    protected LedgerEntry() {
        // required by JPA
    }

    public LedgerEntry(UUID transactionId, Long accountId, Direction direction, long amount) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.direction = direction;
        this.amount = amount;
    }

    public Long getId() {
        return id;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public Long getAccountId() {
        return accountId;
    }

    public Direction getDirection() {
        return direction;
    }

    public long getAmount() {
        return amount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}