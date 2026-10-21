package com.wallet.account;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * One account per (user, currency). Balance is in minor units (sen/cents).
 * Balance changes happen only in the service layer, under a row lock (added in WP 3).
 */
@Entity
@Table(name = "accounts")
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false, updatable = false)
    private Long userId;

    @Column(nullable = false, updatable = false, length = 3)
    private String currency;

    @Column(nullable = false)
    private long balance;

    // Plain column for now, deliberately not @Version: reserved for the optional locking benchmark.
    @Column(nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    protected Account() {
        // required by JPA
    }

    public Account(Long userId, String currency) {
        this.userId = userId;
        this.currency = currency;
    }

    public Long getId() {
        return id;
    }

    public Long getUserId() {
        return userId;
    }

    public String getCurrency() {
        return currency;
    }

    public long getBalance() {
        return balance;
    }

    // Callers must hold the row lock (AccountRepository.lockAllByIdOrdered) before calling these.
    public void credit(long amount) {
        requirePositive(amount);
        this.balance = Math.addExact(this.balance, amount);
    }

    public void debit(long amount) {
        requirePositive(amount);
        if (amount > balance) {
            throw new InsufficientFundsException();
        }
        this.balance -= amount;
    }

    private static void requirePositive(long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
