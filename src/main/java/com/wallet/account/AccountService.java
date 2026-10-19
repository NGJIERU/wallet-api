package com.wallet.account;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.Currency;
import java.util.List;

@Service
public class AccountService {

    private final AccountRepository accounts;

    public AccountService(AccountRepository accounts) {
        this.accounts = accounts;
    }

    public AccountResponse create(Long userId, CreateAccountRequest request) {
        String currency = request.currency();
        try {
            Currency.getInstance(currency);
        } catch (IllegalArgumentException e) {
            throw new InvalidCurrencyException(currency);
        }
        try {
            return AccountResponse.from(accounts.saveAndFlush(new Account(userId, currency)));
        } catch (DataIntegrityViolationException e) {
            // the UNIQUE (user_id, currency) constraint is the source of truth, not a check-then-insert
            throw new DuplicateAccountException();
        }
    }

    public List<AccountResponse> list(Long userId) {
        return accounts.findByUserId(userId).stream().map(AccountResponse::from).toList();
    }

    public AccountResponse get(Long userId, Long accountId) {
        // someone else's account looks identical to a missing one, so ids can't be probed
        return accounts.findById(accountId)
                .filter(a -> a.getUserId().equals(userId))
                .map(AccountResponse::from)
                .orElseThrow(AccountNotFoundException::new);
    }
}