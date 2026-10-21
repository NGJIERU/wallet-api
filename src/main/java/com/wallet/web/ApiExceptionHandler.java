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
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

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

    // A lock timeout on the account row (SQLSTATE 55P03) means "busy, retry". Anything else in this
    // family (a deadlock, say) is not a client conflict. Retrying deadlocks comes in step 3.4.
    @ExceptionHandler(PessimisticLockingFailureException.class)
    ProblemDetail handleLockFailure(PessimisticLockingFailureException e) {
        if (PgErrors.isLockTimeout(e)) {
            return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "The account is busy, retry the request");
        }
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "The request could not be completed");
    }
}