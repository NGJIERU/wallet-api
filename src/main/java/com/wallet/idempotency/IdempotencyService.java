package com.wallet.idempotency;

import com.wallet.common.PgErrors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The claim runs INSIDE the money transaction, so a request that rolls back leaves no key behind.
 * The cost: a concurrent duplicate blocks on the unique-key insert until the first commits (replay)
 * or lock_timeout fires (409).
 */
@Service
public class IdempotencyService {

    private static final Pattern KEY_PATTERN = Pattern.compile("^[!-~]{1,64}$"); // printable ASCII, no spaces

    private record StoredRow(String requestHash, int code, String body) {
    }

    private final JdbcClient jdbc;
    private final long lockTimeoutMs;

    public IdempotencyService(JdbcClient jdbc, @Value("${app.lock-timeout-ms:2000}") long lockTimeoutMs) {
        this.jdbc = jdbc;
        this.lockTimeoutMs = lockTimeoutMs;
    }

    public static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new InvalidIdempotencyKeyException("Idempotency-Key header is required");
        }
        if (!KEY_PATTERN.matcher(key).matches()) {
            throw new InvalidIdempotencyKeyException(
                    "Idempotency-Key must be 1 to 64 printable ASCII characters with no spaces");
        }
    }

    /** SHA-256 hex of method, path and a canonical body, joined with newlines. */
    public static String hash(String method, String path, String canonicalBody) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            byte[] digest = sha.digest((method + "\n" + path + "\n" + canonicalBody).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e); // SHA-256 is guaranteed by every JVM
        }
    }

    /**
     * Must be the FIRST statement of the money transaction. Sets a transaction-local lock_timeout
     * (it also covers the account row locks that follow), then claims the key.
     *
     * @return empty if this request owns the key and should continue; present if it is a replay
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<StoredResponse> claim(long userId, String key, String requestHash) {
        jdbc.sql("SELECT set_config('lock_timeout', ?, true)")
                .param(lockTimeoutMs + "ms")
                .query(String.class)
                .single();

        int inserted;
        try {
            inserted = jdbc.sql("""
                            INSERT INTO idempotency_keys (user_id, key, request_hash, response_code, response_body)
                            VALUES (?, ?, ?, 0, CAST('{}' AS jsonb))
                            ON CONFLICT (user_id, key) DO NOTHING
                            """)
                    .param(userId).param(key).param(requestHash)
                    .update();
        } catch (DataAccessException e) {
            if (PgErrors.isLockTimeout(e)) {
                throw new IdempotencyConflictException();
            }
            throw e;
        }
        if (inserted == 1) {
            return Optional.empty();
        }

        StoredRow row = jdbc.sql("""
                        SELECT request_hash, response_code, response_body::text AS body
                        FROM idempotency_keys WHERE user_id = ? AND key = ?
                        """)
                .param(userId).param(key)
                .query((rs, n) -> new StoredRow(rs.getString("request_hash"), rs.getInt("response_code"), rs.getString("body")))
                .single();
        if (!row.requestHash().equals(requestHash)) {
            throw new IdempotencyKeyReuseException();
        }
        if (row.code() == 0) {
            throw new IllegalStateException("idempotency key was committed without complete() being called");
        }
        return Optional.of(new StoredResponse(row.code(), row.body()));
    }

    /** Stores the response on the key claimed earlier in this same transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void complete(long userId, String key, int responseCode, String responseBodyJson) {
        int updated = jdbc.sql("""
                        UPDATE idempotency_keys SET response_code = ?, response_body = CAST(? AS jsonb)
                        WHERE user_id = ? AND key = ?
                        """)
                .param(responseCode).param(responseBodyJson).param(userId).param(key)
                .update();
        if (updated != 1) {
            throw new IllegalStateException("idempotency key was not claimed in this transaction");
        }
    }
}