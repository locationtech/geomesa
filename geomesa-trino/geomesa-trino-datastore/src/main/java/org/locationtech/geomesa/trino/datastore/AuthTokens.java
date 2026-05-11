/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import java.util.Collection;

/**
 * Validation for authorization tokens against the characters that carry structural
 * meaning somewhere in the auth transport chain:
 *
 * <ul>
 *   <li>{@code |} — joins tokens in the JDBC {@code auths} extra credential;</li>
 *   <li>{@code ,} — joins tokens in the {@code is_visible} SQL literal (and splits them
 *       again inside the UDF), and delimits auth-mapping file lists;</li>
 *   <li>{@code ;} / {@code :} — delimit the Trino JDBC {@code extraCredentials}
 *       {@code name:value;name:value} wire encoding;</li>
 *   <li>whitespace and non-printable/non-ASCII — rejected by the Trino JDBC driver's
 *       extraCredentials validation.</li>
 * </ul>
 *
 * <p>A token containing any of these cannot round-trip losslessly between the datastore's
 * client-side conjunct and the plugin's server-side row filter: it would be silently split
 * into sub-tokens, GRANTING each fragment as an authorization that was never issued. So
 * producers reject such tokens loudly (see {@link #validate}) and resolvers drop them
 * (fail-closed) rather than propagate them.
 *
 * <p>Mirrors the plugin's {@code org.locationtech.geomesa.trino.security.AuthTokens}
 * byte-for-byte — the two modules are isolated (neither is on the other's classpath), so
 * the helper is intentionally duplicated rather than shared. Keep the two in sync.
 */
final class AuthTokens {

    private AuthTokens() {}

    /** True iff the token is non-empty printable ASCII with no delimiter characters. */
    static boolean isValid(String token) {
        if (token == null || token.isEmpty()) {
            return false;
        }
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if (c <= ' ' || c > '~' || c == ',' || c == '|' || c == ';' || c == ':') {
                return false;
            }
        }
        return true;
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
                    "Invalid authorization token '" + token + "': tokens must be non-empty printable"
                    + " ASCII without ',', '|', ';', ':', or whitespace (structural delimiters in the"
                    + " auth transport)");
            }
        }
    }
}
