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

/** One money movement. Immutable once written: every column is updatable = false. */
@Entity
@Table(name = "transactions")
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, updatable = false, length = 16)
    private TransactionType type;

    @Column(nullable = false, updatable = false, length = 3)
    private String currency;

    @Column(nullable = false, updatable = false)
    private long amount;

    @Column(name = "created_by", nullable = false, updatable = false)
    private Long createdBy;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    protected Transaction() {
        // required by JPA
    }

    public Transaction(TransactionType type, String currency, long amount, Long createdBy) {
        this.type = type;
        this.currency = currency;
        this.amount = amount;
        this.createdBy = createdBy;
    }

    public UUID getId() {
        return id;
    }

    public TransactionType getType() {
        return type;
    }

    public String getCurrency() {
        return currency;
    }

    public long getAmount() {
        return amount;
    }

    public Long getCreatedBy() {
        return createdBy;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}