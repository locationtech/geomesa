/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import java.util.Collection;

/**
 * Validates authorization tokens against their join/split delimiter (,):
 *
 * <ul>
 *   <li>{@code ,} — joins tokens in the {@code is_visible} SQL literal, splits the decoded
 *   {@code auths} credential payload, and * delimits auth-mapping file lists. A comma inside
 *   a token would be split into sub-tokens, GRANTING each fragment as an authorization that
 *   was never issued.</li>
 * </ul>
 *
 * <p>Producers reject a comma-bearing token loudly (see {@link #validate}) and resolvers drop
 * it (fail-closed) rather than propagate it.
 *
 * <p>Mirrors the datastore's {@code org.locationtech.geomesa.trino.datastore.AuthTokens}
 * byte-for-byte — the two modules are isolated (neither is on the other's classpath), so
 * the helper is intentionally duplicated rather than shared. Keep the two in sync.
 */
final class AuthTokens {

    private AuthTokens() {}

    /** True iff the token is non-empty and contains no comma — the sole structural delimiter
     *  once the auths payload is base64url-encoded for transport and the {@code is_visible}
     *  literal is quote-escaped. */
    static boolean isValid(String token) {
        return token != null && !token.isEmpty() && token.indexOf(',') < 0;
    }

    /** Throws on the first invalid token. Call before joining tokens for transport —
     *  failing the query loudly beats silently broadening (or narrowing) its auths. */
    static void validate(Collection<String> auths) {
        if (auths == null) {
            return;
        }
        for (String token : auths) {
            if (!isValid(token)) {
                throw new IllegalArgumentException(
                    "Invalid authorization token '" + token + "': tokens must be non-empty and"
                    + " must not contain ','");
            }
        }
    }
}
