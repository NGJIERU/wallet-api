package com.wallet.ratelimit;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Per-request token bucket, placed after bearer authentication in the security chain (created in SecurityConfig,
 * not a bean). Counting rule: it counts REQUESTS, not business operations. It runs before validation, authorization
 * and idempotency, so an invalid request and an idempotent replay each consume a token.
 * Scopes: /auth/** per client IP (X-Forwarded-For is NOT trusted); money writes and every other authenticated
 * endpoint per user id (JWT sub), in separate buckets. /actuator/health/** and /error are never limited.
 * Requests without a valid token (other than /auth/**) are not limited here: the chain answers 401.
 */
public class RateLimitFilter extends OncePerRequestFilter {

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();
    private static final Pattern MONEY_PATH = Pattern.compile("^/accounts/[^/]+/(deposits|withdrawals)$");

    private record Target(String scope, String id, RateLimitProperties.Scope config) {
    }

    private final RateLimiter limiter;
    private final RateLimitProperties props;

    public RateLimitFilter(RateLimiter limiter, RateLimitProperties props) {
        this.limiter = limiter;
        this.props = props;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        Target target = resolve(request);
        if (target == null) {
            chain.doFilter(request, response);
            return;
        }
        RateLimitDecision d = limiter.tryAcquire("rl:" + target.scope() + ":" + target.id(),
                target.config().capacity(), target.config().refillPerSecond());
        if (d.allowed()) {
            if (!d.degraded()) {
                response.setHeader("X-RateLimit-Remaining", Long.toString(d.remaining()));
            }
            chain.doFilter(request, response);
            return;
        }
        reject(request, response, d);
    }

    private Target resolve(HttpServletRequest request) {
        String path = request.getRequestURI().substring(request.getContextPath().length());
        if (path.equals("/error") || path.startsWith("/error/")
                || path.equals("/actuator/health") || path.startsWith("/actuator/health/")) {
            return null;
        }
        if (path.equals("/auth") || path.startsWith("/auth/")) {
            return new Target("auth", request.getRemoteAddr(), props.auth());
        }
        Authentication a = SecurityContextHolder.getContext().getAuthentication();
        if (!(a instanceof JwtAuthenticationToken) || !a.isAuthenticated()) {
            return null;
        }
        boolean money = "POST".equals(request.getMethod())
                && (MONEY_PATH.matcher(path).matches() || path.equals("/transfers"));
        return money ? new Target("money", a.getName(), props.money())
                : new Target("default", a.getName(), props.fallback());
    }

    private void reject(HttpServletRequest request, HttpServletResponse response, RateLimitDecision d) throws IOException {
        long seconds = Math.max(1, (d.retryAfterMs() + 999) / 1000);
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.TOO_MANY_REQUESTS,
                "Rate limit exceeded, retry later");
        try {
            pd.setInstance(URI.create(request.getRequestURI()));
        } catch (IllegalArgumentException ignored) {
            // no instance for an unparsable path
        }
        Map<String, Object> body = new LinkedHashMap<>();
        if (pd.getType() != null) body.put("type", pd.getType().toString());
        if (pd.getTitle() != null) body.put("title", pd.getTitle());
        body.put("status", pd.getStatus());
        if (pd.getDetail() != null) body.put("detail", pd.getDetail());
        if (pd.getInstance() != null) body.put("instance", pd.getInstance().toString());

        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader("Retry-After", Long.toString(seconds));
        response.setHeader("X-RateLimit-Remaining", "0");
        response.setContentType("application/problem+json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(MAPPER.writeValueAsString(body));
    }
}
