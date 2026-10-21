package com.wallet.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.AbstractIntegrationTest;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotencyServiceTest extends AbstractIntegrationTest {

    @Autowired IdempotencyService service;
    @Autowired UserRepository users;
    @Autowired PlatformTransactionManager txManager;
    @Autowired ObjectMapper mapper;

    TransactionTemplate tx;

    @BeforeEach
    void setUp() {
        tx = new TransactionTemplate(txManager);
    }

    @Test
    void claimOutsideATransactionIsRejected() {
        assertThatThrownBy(() -> service.claim(1L, "k", "h"))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void freshKeyIsClaimedThenReplayedWithTheStoredResponse() throws Exception {
        long userId = newUser();
        String hash = IdempotencyService.hash("POST", "/accounts/1/deposits", "amount=1000");
        String body = "{\"id\":\"abc\",\"amount\":1000}";

        Optional<StoredResponse> first = tx.execute(s -> {
            Optional<StoredResponse> claimed = service.claim(userId, "key-1", hash);
            service.complete(userId, "key-1", 201, body);
            return claimed;
        });
        assertThat(first).isEmpty();

        Optional<StoredResponse> replay = tx.execute(s -> service.claim(userId, "key-1", hash));
        assertThat(replay).isPresent();
        assertThat(replay.get().code()).isEqualTo(201);
        // jsonb re-serialises the text, so compare as JSON, not as bytes
        assertThat(mapper.readTree(replay.get().body())).isEqualTo(mapper.readTree(body));
    }

    @Test
    void sameKeyWithADifferentRequestIsRejected() {
        long userId = newUser();
        String hashA = IdempotencyService.hash("POST", "/transfers", "amount=100");
        String hashB = IdempotencyService.hash("POST", "/transfers", "amount=999");

        tx.executeWithoutResult(s -> {
            service.claim(userId, "key-2", hashA);
            service.complete(userId, "key-2", 201, "{}");
        });

        assertThatThrownBy(() -> tx.executeWithoutResult(s -> service.claim(userId, "key-2", hashB)))
                .isInstanceOf(IdempotencyKeyReuseException.class);
    }

    @Test
    void theSameKeyOwnedByTwoUsersDoesNotCollide() {
        long userA = newUser();
        long userB = newUser();
        String hash = IdempotencyService.hash("POST", "/transfers", "amount=100");

        Optional<StoredResponse> claimA = tx.execute(s -> service.claim(userA, "shared-key", hash));
        Optional<StoredResponse> claimB = tx.execute(s -> service.claim(userB, "shared-key", hash));

        assertThat(claimA).isEmpty();
        assertThat(claimB).isEmpty();
    }

    @Test
    void aRolledBackClaimLeavesNoStuckKey() {
        long userId = newUser();
        String hash = IdempotencyService.hash("POST", "/transfers", "amount=100");

        assertThatThrownBy(() -> tx.executeWithoutResult(s -> {
            service.claim(userId, "key-3", hash);
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        // the retry owns the key again, not a replay and not a conflict
        Optional<StoredResponse> retry = tx.execute(s -> {
            Optional<StoredResponse> claimed = service.claim(userId, "key-3", hash);
            service.complete(userId, "key-3", 201, "{}");
            return claimed;
        });
        assertThat(retry).isEmpty();
    }

    @Test
    void validateKeyRejectsMissingBlankTooLongAndSpaces() {
        assertThatThrownBy(() -> IdempotencyService.validateKey(null)).isInstanceOf(InvalidIdempotencyKeyException.class);
        assertThatThrownBy(() -> IdempotencyService.validateKey("")).isInstanceOf(InvalidIdempotencyKeyException.class);
        assertThatThrownBy(() -> IdempotencyService.validateKey("   ")).isInstanceOf(InvalidIdempotencyKeyException.class);
        assertThatThrownBy(() -> IdempotencyService.validateKey("a".repeat(65))).isInstanceOf(InvalidIdempotencyKeyException.class);
        assertThatThrownBy(() -> IdempotencyService.validateKey("has space")).isInstanceOf(InvalidIdempotencyKeyException.class);

        assertThatCode(() -> IdempotencyService.validateKey("a".repeat(64))).doesNotThrowAnyException();
        assertThatCode(() -> IdempotencyService.validateKey(UUID.randomUUID().toString())).doesNotThrowAnyException();
    }

    @Test
    void hashIsStableAndSensitiveToEveryPart() {
        String base = IdempotencyService.hash("POST", "/transfers", "a");
        assertThat(base).hasSize(64);
        assertThat(IdempotencyService.hash("POST", "/transfers", "a")).isEqualTo(base);
        assertThat(IdempotencyService.hash("POST", "/transfers", "b")).isNotEqualTo(base);
        assertThat(IdempotencyService.hash("POST", "/other", "a")).isNotEqualTo(base);
        assertThat(IdempotencyService.hash("PUT", "/transfers", "a")).isNotEqualTo(base);
    }

    private long newUser() {
        return users.saveAndFlush(new User("idem-" + UUID.randomUUID() + "@example.test", "x")).getId();
    }
}