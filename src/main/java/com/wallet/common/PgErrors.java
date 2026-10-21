package com.wallet.common;

import java.sql.SQLException;

public final class PgErrors {

    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String DEADLOCK = "40P01";
    private static final String SERIALIZATION_FAILURE = "40001";

    private PgErrors() {
    }

    /** True if a Postgres lock timeout (SQLSTATE 55P03) appears anywhere in the cause chain. */
    public static boolean isLockTimeout(Throwable t) {
        return hasSqlState(t, LOCK_NOT_AVAILABLE);
    }

    /** True if Postgres killed the transaction as a deadlock victim (SQLSTATE 40P01). */
    public static boolean isDeadlock(Throwable t) {
        return hasSqlState(t, DEADLOCK);
    }

    /**
     * Failures where running the whole transaction again is the right response: a deadlock victim
     * or a serialization failure. A lock timeout is deliberately not in this list: the lock holder
     * is still busy, so the client should retry later, not the server immediately.
     */
    public static boolean isRetryable(Throwable t) {
        return hasSqlState(t, DEADLOCK, SERIALIZATION_FAILURE);
    }

    private static boolean hasSqlState(Throwable t, String... states) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SQLException se) {
                for (String state : states) {
                    if (state.equals(se.getSQLState())) {
                        return true;
                    }
                }
            }
            if (c.getCause() == c) {
                break;
            }
        }
        return false;
    }
}