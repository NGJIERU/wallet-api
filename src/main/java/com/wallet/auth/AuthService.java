package com.wallet.auth;

import com.wallet.config.JwtProperties;
import com.wallet.user.User;
import com.wallet.user.UserRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Locale;

@Service
public class AuthService {

    private final UserRepository users;
    private final PasswordEncoder passwordEncoder;
    private final JwtEncoder jwtEncoder;
    private final JwtProperties jwtProperties;

    public AuthService(UserRepository users, PasswordEncoder passwordEncoder,
                       JwtEncoder jwtEncoder, JwtProperties jwtProperties) {
        this.users = users;
        this.passwordEncoder = passwordEncoder;
        this.jwtEncoder = jwtEncoder;
        this.jwtProperties = jwtProperties;
    }

    public RegisterResponse register(RegisterRequest request) {
        String email = normalize(request.email());
        String hash = passwordEncoder.encode(request.password());
        try {
            User saved = users.saveAndFlush(new User(email, hash));
            return new RegisterResponse(saved.getId(), saved.getEmail());
        } catch (DataIntegrityViolationException e) {
            // the UNIQUE constraint on email is the source of truth, not a check-then-insert
            throw new DuplicateEmailException();
        }
    }

    public TokenResponse login(LoginRequest request) {
        // unknown email and wrong password throw the same exception, so callers can't tell which emails exist
        User user = users.findByEmail(normalize(request.email()))
                .orElseThrow(InvalidCredentialsException::new);
        if (!passwordEncoder.matches(request.password(), user.getPasswordHash())) {
            throw new InvalidCredentialsException();
        }

        Instant now = Instant.now();
        long expiresInSeconds = jwtProperties.accessTokenMinutes() * 60;
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .subject(String.valueOf(user.getId()))
                .issuedAt(now)
                .expiresAt(now.plusSeconds(expiresInSeconds))
                .build();
        // must be HS256 explicitly: NimbusJwtEncoder defaults to RS256, which fails with a secret key
        JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).build();
        String token = jwtEncoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
        return new TokenResponse(token, "Bearer", expiresInSeconds);
    }

    private static String normalize(String email) {
        return email.trim().toLowerCase(Locale.ROOT);
    }
}