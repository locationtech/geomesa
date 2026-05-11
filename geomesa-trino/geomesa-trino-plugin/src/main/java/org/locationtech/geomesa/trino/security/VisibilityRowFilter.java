/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import java.util.List;

/**
 * Builds the SQL row-filter expression evaluated by the {@code is_visible} UDF.
 *
 * <p>Mirrors the datastore's {@code TrinoFeatureSource.visibilityConjunct}
 * (geomesa-trino-datastore) byte-for-byte — the two modules are isolated
 * (the datastore is not on the plugin classpath), so the ~3-line helper is
 * intentionally duplicated rather than shared. Keep the two in sync.
 */
final class VisibilityRowFilter {

    private VisibilityRowFilter() {}

    /**
     * {@code is_visible("<col>", '<auths-csv>')}. The column identifier is
     * double-quoted (embedded {@code "} doubled) and the auths literal is
     * single-quote-escaped (embedded {@code '} doubled). Empty auths emit
     * {@code ''} — fail-closed (only unrestricted rows pass).
     *
     * @throws IllegalArgumentException if a token contains a transport delimiter —
     *         the UDF would re-split it into auths that were never issued (see
     *         {@link AuthTokens})
     */
    static String conjunct(String visColumn, List<String> auths) {
        AuthTokens.validate(auths);
        String column = "\"" + visColumn.replace("\"", "\"\"") + "\"";
        String literal = String.join(",", auths).replace("'", "''");
        return "is_visible(" + column + ", '" + literal + "')";
    }
}
