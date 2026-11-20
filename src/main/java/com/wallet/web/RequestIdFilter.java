package com.wallet.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerMapping;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Outermost servlet filter (order -200, below Spring Security's chain at -100) so it also wraps 401 and 429
 * responses. It is the one place that sets the MDC requestId, sets the X-Request-Id response header, writes the
 * completion line and clears the MDC (in finally). The userId MDC key is set later by an in-chain filter and is
 * only read and cleared here. Logs no body, header value, query string or token. Health probes get the header
 * but no completion line.
 */
@Component
@Order(RequestIdFilter.ORDER)
public class RequestIdFilter extends OncePerRequestFilter {

    public static final int ORDER = -200;
    public static final String HEADER = "X-Request-Id";
    public static final String MDC_REQUEST_ID = "requestId";
    public static final String MDC_USER_ID = "userId";

    private static final Logger log = LoggerFactory.getLogger(RequestIdFilter.class);
    // matches() needs the whole value to fit: a trailing newline or any control character is rejected
    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String supplied = request.getHeader(HEADER);
        String id = supplied != null && SAFE_ID.matcher(supplied).matches() ? supplied : UUID.randomUUID().toString();
        MDC.put(MDC_REQUEST_ID, id);
        response.setHeader(HEADER, id);
        long start = System.nanoTime();
        try {
            chain.doFilter(request, response);
        } finally {
            try {
                String path = request.getRequestURI().substring(request.getContextPath().length());
                if (!isHealth(path)) {
                    String userId = MDC.get(MDC_USER_ID);
                    long ms = (System.nanoTime() - start) / 1_000_000;
                    String shown = shownPath(request, path);
                    if (userId == null) {
                        log.info("request completed method={} path={} status={} durationMs={} requestId={}",
                                request.getMethod(), shown, response.getStatus(), ms, id);
                    } else {
                        log.info("request completed method={} path={} status={} durationMs={} requestId={} userId={}",
                                request.getMethod(), shown, response.getStatus(), ms, id, userId);
                    }
                }
            } finally {
                MDC.remove(MDC_REQUEST_ID);
                MDC.remove(MDC_USER_ID);
            }
        }
    }

    /** The matched pattern if a handler matched, else the raw path (getRequestURI has no query string). */
    private static String shownPath(HttpServletRequest request, String rawPath) {
        Object pattern = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        return pattern instanceof String s && !s.equals("/**") ? s : rawPath;
    }

    private static boolean isHealth(String path) {
        return path.equals("/actuator/health") || path.startsWith("/actuator/health/");
    }
}
