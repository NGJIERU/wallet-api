package com.wallet.transaction;

import com.wallet.common.TransactionRetry;
import com.wallet.idempotency.StoredResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Deliberately NOT @Transactional. Each attempt must open its own transaction (inside
 * TransferTransaction), otherwise a retry would run in an already-aborted transaction.
 */
@Service
public class TransferService {

    private final TransferTransaction transaction;
    private final TransactionRetry retry;

    public TransferService(TransferTransaction transaction,
                           @Value("${app.transfer.retry-attempts:3}") int attempts,
                           @Value("${app.transfer.retry-backoff-ms:25}") long backoffMs) {
        this.transaction = transaction;
        this.retry = new TransactionRetry(attempts, backoffMs);
    }

    public StoredResponse transfer(long userId, String key, long fromId, long toId, long amount) {
        return retry.run(() -> transaction.execute(userId, key, fromId, toId, amount));
    }
}