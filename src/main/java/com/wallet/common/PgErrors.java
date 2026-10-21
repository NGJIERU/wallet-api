package com.wallet.common;

import java.sql.SQLException;

public final class PgErrors {

    private static final String LOCK_NOT_AVAILABLE = "55P03";

    private PgErrors() {
    }

    /** True if a Postgres lock timeout (SQLSTATE 55P03) appears anywhere in the cause chain. */
    public static boolean isLockTimeout(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SQLException se && LOCK_NOT_AVAILABLE.equals(se.getSQLState())) {
                return true;
            }
            if (c.getCause() == c) {
                break;
            }
        }
        return false;
    }
}