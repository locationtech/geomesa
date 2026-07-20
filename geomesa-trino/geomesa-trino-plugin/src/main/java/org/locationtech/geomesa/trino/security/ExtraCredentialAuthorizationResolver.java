/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.spi.security.ConnectorIdentity;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AuthorizationResolver} that reads a session's authorization tokens
 * from a Trino <em>extra credential</em> carrying a base64url-encoded
 * token list. The decoded payload is a comma or whitespace delimited list.
 * Base64url keeps the wire value free of the extraCredentials pair delimiters
 * by construction. Intended for deployments fronted by a trusted service mesh
 * that authenticates the caller and injects the authoritative token set:
 * the mesh base64url-encodes its identity header (e.g. {@code x-auths}) into Trino's
 * {@code X-Trino-Extra-Credential} client header, which Trino exposes here
 * via {@link ConnectorIdentity#getExtraCredentials()}.
 *
 * <p>Because the tokens flow through unchanged there is no mapping file to
 * maintain and nothing to drift from the platform's clearances. Selected with:
 * <pre>
 *   geomesa.security.auth-resolver=org.locationtech.geomesa.trino.security.ExtraCredentialAuthorizationResolver
 *   geomesa.security.auths-credential=auths   # optional; the extra-credential name (default "auths")
 * </pre>
 *
 * <p>A missing or empty credential resolves to an empty set — fail-closed (the
 * row filter then admits only unrestricted rows). This trusts the extra
 * credential implicitly, so the deployment MUST ensure only trusted callers can
 * set it. When the transport can't be locked down to the trusted caller (e.g. no
 * mesh AuthorizationPolicy), configure a shared secret via
 * {@code geomesa.security.auths-secret}: the caller must then also present a
 * matching {@code secret} extra credential or NO auths are honored. The secret
 * value must not contain a comma or colon (the Trino extraCredentials wire format
 * uses {@code name:value,name:value}); generate it hex/base64url.
 */
public final class ExtraCredentialAuthorizationResolver implements AuthorizationResolver {

    private static final Logger LOG =
        LoggerFactory.getLogger(ExtraCredentialAuthorizationResolver.class);

    /** Catalog property naming the extra credential that carries the tokens. */
    static final String CREDENTIAL_KEY = "geomesa.security.auths-credential";
    static final String DEFAULT_CREDENTIAL = "auths";

    /** Optional shared secret; when set, callers must present a matching value in
     *  the secret credential (below) or they resolve to no auths (fail-closed). */
    static final String SECRET_KEY = "geomesa.security.auths-secret";
    static final String SECRET_CREDENTIAL_KEY = "geomesa.security.secret-credential";
    static final String DEFAULT_SECRET_CREDENTIAL = "secret";

    private final String credential;
    private final String secretCredential;
    private final byte[] expectedSecret;  // null => no secret gating

    /**
     * Builds a resolver from catalog configuration.
     *
     * @param config catalog properties (credential name, optional shared secret)
     */
    public ExtraCredentialAuthorizationResolver(Map<String, String> config) {
        this.credential = config.getOrDefault(CREDENTIAL_KEY, DEFAULT_CREDENTIAL);
        this.secretCredential = config.getOrDefault(SECRET_CREDENTIAL_KEY, DEFAULT_SECRET_CREDENTIAL);
        String secret = config.get(SECRET_KEY);
        this.expectedSecret = (secret == null || secret.isEmpty())
            ? null : secret.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Authorizations read from the identity's extra credential; empty set if the
     * credential is absent or the required secret does not match (fail-closed).
     *
     * @param identity the Trino session identity carrying the extra credentials
     * @return authorization tokens from the credential; empty set if none
     */
    @Override
    public Set<String> authorizationsFor(ConnectorIdentity identity) {
        if (expectedSecret != null && !secretMatches(identity)) {
            return Set.of();  // secret required but absent/wrong — fail-closed
        }
        Set<String> auths = new LinkedHashSet<>();
        addTokens(auths, identity.getExtraCredentials().get(credential));
        return auths;
    }

    /** Constant-time comparison of the presented secret against the expected one. */
    private boolean secretMatches(ConnectorIdentity identity) {
        String presented = identity.getExtraCredentials().get(secretCredential);
        if (presented == null) {
            return false;
        }
        return MessageDigest.isEqual(expectedSecret, presented.getBytes(StandardCharsets.UTF_8));
    }

    private static void addTokens(Set<String> out, String encoded) {
        if (encoded == null) return;
        String decoded;
        try {
            decoded = new String(Base64.getUrlDecoder().decode(encoded.trim()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            LOG.warn("Authorization extra credential is not valid base64url; resolving to no auths");
            return;
        }
        for (String token : decoded.split(",")) {
            String t = token.trim();
            if (t.isEmpty()) continue;
            if (!AuthTokens.isValid(t)) {
                LOG.warn("Dropping invalid authorization token '" + t + "' from extra credential");
                continue;
            }
            out.add(t);
        }
    }
}
