package com.wallet.concurrency;

import com.wallet.common.ContentionException;
import com.wallet.common.PgErrors;
import com.wallet.account.InsufficientFundsException;
import com.wallet.idempotency.IdempotencyConflictException;
import com.wallet.idempotency.IdempotencyKeyReuseException;
import org.springframework.dao.PessimisticLockingFailureException;

enum Outcome {
    OK, INSUFFICIENT, KEY_REUSE, BUSY, UNEXPECTED;

    /** Walks the cause chain top-down; the first recognised type wins. */
    static Outcome classify(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof InsufficientFundsException) return INSUFFICIENT;
            if (c instanceof IdempotencyKeyReuseException) return KEY_REUSE;
            if (c instanceof IdempotencyConflictException || c instanceof ContentionException) return BUSY;
            if (c instanceof PessimisticLockingFailureException && PgErrors.isLockTimeout(c)) return BUSY;
            if (c.getCause() == c) break;
        }
        return UNEXPECTED;
    }
}