/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.spi.security.HeaderAuthenticator;
import io.trino.spi.security.HeaderAuthenticatorFactory;

import java.util.Map;

/**
 * Registers the {@code trusted-principal-header} header authenticator. Selected by the
 * coordinator's {@code etc/header-authenticator.properties}:
 * <pre>
 *   header-authenticator.name=trusted-principal-header
 *   trusted-principal-header=&lt;request header carrying the authenticated principal&gt;
 *   allow-trino-user-fallback=false   # optional; default false (fail-closed)
 * </pre>
 * The user-principal header name is REQUIRED configuration (no default): the deployment
 * specifies which trusted, upstream-injected request header carries the authenticated
 * principal. (The {@code header-authenticator.name} key is consumed by Trino and is not
 * passed in the config map.) Group membership for authorization is handled by a Trino group
 * provider, not here.
 *
 * <p>{@code allow-trino-user-fallback} (optional, default {@code false}) enables falling back
 * to the client-controlled {@code X-Trino-User} header when the trusted header is absent — set
 * it {@code true} ONLY when the coordinator is network-confined to a trusted caller (see
 * {@link TrustedPrincipalHeaderAuthenticator} for the trust-boundary rationale).
 */
public class TrustedPrincipalHeaderAuthenticatorFactory
        implements HeaderAuthenticatorFactory
{
    private static final String NAME = "trusted-principal-header";
    private static final String USER_HEADER_PROPERTY = "trusted-principal-header";
    /** Optional; default false (fail-closed). Enables the client-controlled X-Trino-User
     *  fallback — only set true when the coordinator is network-confined to a trusted caller. */
    private static final String ALLOW_TRINO_USER_FALLBACK_PROPERTY = "allow-trino-user-fallback";

    /** Default constructor. */
    public TrustedPrincipalHeaderAuthenticatorFactory() {}

    /**
     * Name of this header authenticator.
     *
     * @return the authenticator name ({@code trusted-principal-header})
     */
    @Override
    public String getName()
    {
        return NAME;
    }

    /**
     * Creates the header authenticator from the supplied configuration.
     *
     * @param config authenticator configuration (must name the principal header)
     * @return the configured header authenticator
     */
    @Override
    public HeaderAuthenticator create(Map<String, String> config)
    {
        String userHeader = config.get(USER_HEADER_PROPERTY);
        if (userHeader == null || userHeader.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing required configuration '" + USER_HEADER_PROPERTY + "': set it to the name "
                    + "of the trusted request header that carries the authenticated principal.");
        }
        boolean allowFallback = Boolean.parseBoolean(
                config.getOrDefault(ALLOW_TRINO_USER_FALLBACK_PROPERTY, "false"));
        return new TrustedPrincipalHeaderAuthenticator(userHeader.trim(), allowFallback);
    }
}
