package com.wallet.transaction;

import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class TransactionQueryController {

    private final TransactionQueryService queries;

    public TransactionQueryController(TransactionQueryService queries) {
        this.queries = queries;
    }

    @GetMapping("/accounts/{accountId}/transactions")
    public TransactionHistoryPage history(@AuthenticationPrincipal Jwt jwt, @PathVariable Long accountId,
                                          @RequestParam(required = false) Long cursor,
                                          @RequestParam(defaultValue = "20") int limit) {
        return queries.history(userId(jwt), accountId, cursor, limit);
    }

    @GetMapping("/transactions/{id}")
    public TransactionDetailResponse get(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return queries.get(userId(jwt), id);
    }

    private static Long userId(Jwt jwt) {
        return Long.valueOf(jwt.getSubject());
    }
}