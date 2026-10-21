package com.wallet.web;

import com.wallet.account.AccountNotFoundException;
import com.wallet.account.DestinationAccountNotFoundException;
import com.wallet.account.InsufficientFundsException;
import com.wallet.common.ContentionException;
import com.wallet.idempotency.IdempotencyConflictException;
import com.wallet.idempotency.IdempotencyKeyReuseException;
import com.wallet.idempotency.InvalidIdempotencyKeyException;
import com.wallet.transaction.InvalidPageRequestException;
import com.wallet.transaction.InvalidTransferException;
import com.wallet.transaction.TransactionNotFoundException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class ApiExceptionHandlerTest {

    private final ApiExceptionHandler handler = new ApiExceptionHandler();

    @Test
    void domainErrorsMapToTheDocumentedStatuses() {
        assertThat(handler.handleAccountNotFound(new AccountNotFoundException()).getStatus()).isEqualTo(404);
        assertThat(handler.handleDestinationNotFound(new DestinationAccountNotFoundException(7)).getStatus()).isEqualTo(404);
        assertThat(handler.handleTransactionNotFound(new TransactionNotFoundException()).getStatus()).isEqualTo(404);
        assertThat(handler.handleInsufficientFunds(new InsufficientFundsException()).getStatus()).isEqualTo(422);
        assertThat(handler.handleIdempotencyKeyReuse(new IdempotencyKeyReuseException()).getStatus()).isEqualTo(422);
        assertThat(handler.handleIdempotencyConflict(new IdempotencyConflictException()).getStatus()).isEqualTo(409);
        assertThat(handler.handleInvalidIdempotencyKey(new InvalidIdempotencyKeyException("x")).getStatus()).isEqualTo(400);
        assertThat(handler.handleInvalidTransfer(new InvalidTransferException("x")).getStatus()).isEqualTo(400);
        assertThat(handler.handleInvalidPageRequest(new InvalidPageRequestException("x")).getStatus()).isEqualTo(400);
        assertThat(handler.handleContention(new ContentionException(new RuntimeException())).getStatus()).isEqualTo(409);
    }

    @Test
    void aLockTimeoutIs409ButAnyOtherLockingFailureIs500() {
        var timeout = new CannotAcquireLockException("x", new SQLException("lock timeout", "55P03"));
        var other = new CannotAcquireLockException("x", new SQLException("boom", "XX000"));

        assertThat(handler.handleLockFailure(timeout).getStatus()).isEqualTo(409);
        assertThat(handler.handleLockFailure(timeout).getDetail()).contains("busy");
        assertThat(handler.handleLockFailure(other).getStatus()).isEqualTo(500);
    }

    @Test
    void unexpectedErrorsAre500AndNeverLeakTheirMessage() {
        var problem = handler.handleUnexpected(new RuntimeException("relation \"ledger_entries\" is corrupt"));

        assertThat(problem.getStatus()).isEqualTo(500);
        assertThat(problem.getDetail()).doesNotContain("ledger_entries");
    }
}