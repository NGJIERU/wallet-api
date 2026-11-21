package com.wallet.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Puts the authenticated user id (JWT sub) into the MDC for the completion line. Created in SecurityConfig after
 * bearer authentication and deliberately NOT a bean (a Filter bean would also be registered in the servlet
 * container and run twice). It never clears the MDC: RequestIdFilter, the outer filter, does that.
 */
public class UserIdMdcFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        Authentication a = SecurityContextHolder.getContext().getAuthentication();
        if (a instanceof JwtAuthenticationToken && a.isAuthenticated()) {
            MDC.put(RequestIdFilter.MDC_USER_ID, a.getName());
        }
        chain.doFilter(request, response);
    }
}
