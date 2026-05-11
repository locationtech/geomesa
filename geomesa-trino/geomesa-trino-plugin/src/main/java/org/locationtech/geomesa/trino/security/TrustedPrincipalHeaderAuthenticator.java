/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.HeaderAuthenticator;

import java.security.Principal;
import java.util.List;

/**
 * Passwordless header authenticator for {@code http-server.authentication.type=HEADER}.
 *
 * <p>Trino ships no built-in header authenticator; this one establishes the request principal
 * from a trusted, upstream-authenticated request header whose name is supplied by configuration
 * ({@code trusted-principal-header}). It is selected by {@code header-authenticator.name=trusted-principal-header}.
 *
 * <p><strong>Security model / trust boundary.</strong> The principal established here drives
 * row-level entitlements (see {@code AuthorizationResolver}), so the authenticity of the header
 * is load-bearing. The {@code trusted-principal-header} is expected to be injected by an upstream
 * gateway/mesh that has already authenticated the caller and that STRIPS any client-supplied copy.
 *
 * <p>{@code X-Trino-User} is a <em>client-controlled</em> header (the JDBC driver sets it to the
 * session user), so trusting it is only safe when the coordinator is network-confined such that
 * the sole reachable caller is a trusted intermediary (e.g. the GeoTools datastore behind a
 * NetworkPolicy). Because that confinement is a deployment property this authenticator can't
 * verify, the {@code X-Trino-User} fallback is <strong>disabled by default (fail-closed)</strong>
 * and must be explicitly enabled with {@code allow-trino-user-fallback=true}. With the fallback
 * off, a request lacking the trusted header is denied.
 */
public class TrustedPrincipalHeaderAuthenticator
        implements HeaderAuthenticator
{
    /** Standard Trino session-user header; the JDBC driver always sends it. Client-controlled. */
    private static final String TRINO_USER_HEADER = "X-Trino-User";

    private final String userHeader;
    private final boolean allowTrinoUserFallback;

    /**
     * Builds a fail-closed authenticator (no {@code X-Trino-User} fallback).
     *
     * @param userHeader name of the trusted request header carrying the principal
     */
    public TrustedPrincipalHeaderAuthenticator(String userHeader)
    {
        this(userHeader, false);
    }

    /**
     * Builds an authenticator that reads the principal from the given trusted header.
     *
     * @param userHeader name of the trusted request header carrying the principal
     * @param allowTrinoUserFallback whether to fall back to the client-controlled
     *        {@code X-Trino-User} header when the trusted header is absent — only safe when
     *        the coordinator is network-confined to a trusted caller (see class javadoc)
     */
    public TrustedPrincipalHeaderAuthenticator(String userHeader, boolean allowTrinoUserFallback)
    {
        this.userHeader = userHeader;
        this.allowTrinoUserFallback = allowTrinoUserFallback;
    }

    /**
     * Establishes the request principal from the configured trusted header. Falls back to
     * {@code X-Trino-User} only when {@code allow-trino-user-fallback} is enabled; otherwise
     * a request without the trusted header is denied (fail-closed).
     *
     * @param headers the request headers
     * @return the authenticated principal
     */
    @Override
    public Principal createAuthenticatedPrincipal(Headers headers)
    {
        String user = firstNonBlank(headers.getHeader(userHeader));
        if (user == null && allowTrinoUserFallback) {
            user = firstNonBlank(headers.getHeader(TRINO_USER_HEADER));
        }
        if (user == null) {
            throw new AccessDeniedException(allowTrinoUserFallback
                    ? "No " + userHeader + " or " + TRINO_USER_HEADER + " header present"
                    : "No " + userHeader + " header present");
        }
        return new BasicPrincipal(user);
    }

    private static String firstNonBlank(List<String> values)
    {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }
}
