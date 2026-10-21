package com.wallet.web;

import com.wallet.auth.DuplicateEmailException;
import com.wallet.auth.InvalidCredentialsException;
import com.wallet.account.AccountNotFoundException;
import com.wallet.account.DuplicateAccountException;
import com.wallet.account.InvalidCurrencyException;
import com.wallet.account.InsufficientFundsException;
import com.wallet.common.PgErrors;
import com.wallet.idempotency.IdempotencyConflictException;
import com.wallet.idempotency.IdempotencyKeyReuseException;
import com.wallet.idempotency.InvalidIdempotencyKeyException;
import com.wallet.account.DestinationAccountNotFoundException;
import com.wallet.common.ContentionException;
import com.wallet.transaction.InvalidPageRequestException;
import com.wallet.transaction.InvalidTransferException;
import com.wallet.transaction.TransactionNotFoundException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(DuplicateEmailException.class)
    ProblemDetail handleDuplicateEmail(DuplicateEmailException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(InvalidCredentialsException.class)
    ProblemDetail handleInvalidCredentials(InvalidCredentialsException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNAUTHORIZED, e.getMessage());
    }

    @ExceptionHandler(AccountNotFoundException.class)
    ProblemDetail handleAccountNotFound(AccountNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(DuplicateAccountException.class)
    ProblemDetail handleDuplicateAccount(DuplicateAccountException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(InvalidCurrencyException.class)
    ProblemDetail handleInvalidCurrency(InvalidCurrencyException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(InvalidIdempotencyKeyException.class)
    ProblemDetail handleInvalidIdempotencyKey(InvalidIdempotencyKeyException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(IdempotencyKeyReuseException.class)
    ProblemDetail handleIdempotencyKeyReuse(IdempotencyKeyReuseException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    ProblemDetail handleIdempotencyConflict(IdempotencyConflictException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(InsufficientFundsException.class)
    ProblemDetail handleInsufficientFunds(InsufficientFundsException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    }

    @ExceptionHandler(DestinationAccountNotFoundException.class)
    ProblemDetail handleDestinationNotFound(DestinationAccountNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(InvalidTransferException.class)
    ProblemDetail handleInvalidTransfer(InvalidTransferException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(TransactionNotFoundException.class)
    ProblemDetail handleTransactionNotFound(TransactionNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(InvalidPageRequestException.class)
    ProblemDetail handleInvalidPageRequest(InvalidPageRequestException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    // Deadlock or serialization retries ran out (see TransactionRetry): the client should retry later.
    @ExceptionHandler(ContentionException.class)
    ProblemDetail handleContention(ContentionException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    // A lock timeout on the account row (SQLSTATE 55P03) means "busy, retry". Deadlocks are retried by
    // TransactionRetry before they get here; if retries run out they become ContentionException (409).
    @ExceptionHandler(PessimisticLockingFailureException.class)
    ProblemDetail handleLockFailure(PessimisticLockingFailureException e) {
        if (PgErrors.isLockTimeout(e)) {
            return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "The account is busy, retry the request");
        }
        log.error("Unexpected locking failure", e);
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "The request could not be completed");
    }

    // Last resort: every unexpected error gets the same ProblemDetail shape, and the message
    // (which may name tables or SQL) is logged, never sent to the client.
    // Spring MVC's own exceptions (bad JSON, missing header, ...) still go to the parent class handlers.
    @ExceptionHandler(Exception.class)
    ProblemDetail handleUnexpected(Exception e) {
        log.error("Unhandled exception", e);
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "The request could not be completed");
    }
}