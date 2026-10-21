package com.wallet.transaction;

import com.wallet.idempotency.StoredResponse;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/transfers")
public class TransferController {

    private final TransferService transfers;

    public TransferController(TransferService transfers) {
        this.transfers = transfers;
    }

    // Header optional on purpose: a missing key is rejected by the service with our own 400.
    @PostMapping
    public ResponseEntity<String> transfer(@AuthenticationPrincipal Jwt jwt,
                                           @RequestHeader(value = "Idempotency-Key", required = false) String key,
                                           @Valid @RequestBody TransferRequest request) {
        StoredResponse r = transfers.transfer(Long.valueOf(jwt.getSubject()), key,
                request.fromAccountId(), request.toAccountId(), request.amount());
        // the stored response is already JSON text, so it is returned as-is (a replay is the stored bytes)
        return ResponseEntity.status(r.code()).contentType(MediaType.APPLICATION_JSON).body(r.body());
    }
}