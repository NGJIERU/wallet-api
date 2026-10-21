package com.wallet.common;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class PgErrorsTest {

    @Test
    void lockTimeoutIsFoundAnywhereInTheCauseChain() {
        Throwable wrapped = new RuntimeException("outer",
                new IllegalStateException("middle", new SQLException("canceling statement due to lock timeout", "55P03")));
        assertThat(PgErrors.isLockTimeout(wrapped)).isTrue();
    }

    @Test
    void otherErrorsAreNotLockTimeouts() {
        assertThat(PgErrors.isLockTimeout(new RuntimeException("x", new SQLException("deadlock detected", "40P01")))).isFalse();
        assertThat(PgErrors.isLockTimeout(new RuntimeException("no cause"))).isFalse();
    }
}