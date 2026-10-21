package com.wallet.transaction;

import com.wallet.idempotency.StoredResponse;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/accounts/{accountId}")
public class MoneyController {

    private final MoneyService money;

    public MoneyController(MoneyService money) {
        this.money = money;
    }

    // The header is optional here on purpose: a missing key is rejected by the service
    // with our own 400, so all key errors share one format.
    @PostMapping("/deposits")
    public ResponseEntity<String> deposit(@AuthenticationPrincipal Jwt jwt, @PathVariable Long accountId,
                                          @RequestHeader(value = "Idempotency-Key", required = false) String key,
                                          @Valid @RequestBody MoneyRequest request) {
        return respond(money.deposit(userId(jwt), accountId, key, request.amount()));
    }

    @PostMapping("/withdrawals")
    public ResponseEntity<String> withdraw(@AuthenticationPrincipal Jwt jwt, @PathVariable Long accountId,
                                           @RequestHeader(value = "Idempotency-Key", required = false) String key,
                                           @Valid @RequestBody MoneyRequest request) {
        return respond(money.withdraw(userId(jwt), accountId, key, request.amount()));
    }

    // The stored response is already JSON text, so it is returned as-is (a replay is the stored bytes).
    private static ResponseEntity<String> respond(StoredResponse r) {
        return ResponseEntity.status(r.code()).contentType(MediaType.APPLICATION_JSON).body(r.body());
    }

    private static Long userId(Jwt jwt) {
        return Long.valueOf(jwt.getSubject());
    }
}