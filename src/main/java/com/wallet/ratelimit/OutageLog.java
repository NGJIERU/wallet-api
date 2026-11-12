package com.wallet.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Outage logging policy: one WARN per window while the limiter is degraded (with the exception class and how many
 * failures were suppressed since the previous line), one INFO on the first success after a failure, and nothing
 * on a healthy path. The clock is injectable so the window is testable.
 */
public class OutageLog {

    private static final Logger log = LoggerFactory.getLogger(OutageLog.class);

    private final Clock clock;
    private final Duration window;
    private volatile boolean degraded;
    private Instant lastWarn;
    private long suppressed;

    public OutageLog(Clock clock, Duration window) {
        this.clock = clock;
        this.window = window;
    }

    void failure(Throwable cause) {
        synchronized (this) {
            degraded = true;
            Instant now = clock.instant();
            if (lastWarn == null || !now.isBefore(lastWarn.plus(window))) {
                log.warn("Rate limiter degraded, requests are allowed without limiting (fail-open). Cause: {}. "
                        + "{} further failures suppressed since the last warning", cause.getClass().getName(), suppressed);
                suppressed = 0;
                lastWarn = now;
            } else {
                suppressed++;
            }
        }
    }

    void success() {
        if (!degraded) {
            return; // healthy path: no lock, no log line
        }
        synchronized (this) {
            if (!degraded) {
                return;
            }
            degraded = false;
            log.info("Rate limiter recovered, limiting is enforced again ({} failures were suppressed since the last warning)",
                    suppressed);
            suppressed = 0;
            lastWarn = null;
        }
    }
}
