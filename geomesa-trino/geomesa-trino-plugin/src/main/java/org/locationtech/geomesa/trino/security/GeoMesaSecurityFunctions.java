/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.accumulo.access.AccessEvaluator;
import org.apache.accumulo.access.Authorizations;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Row-entitlement UDF: same semantics as geomesa-security's
 * VisibilityUtils — NULL/empty visibility unrestricted, invalid expressions
 * are hidden (fail-closed).
 */
public final class GeoMesaSecurityFunctions {

    // Worker-lifetime cache; auth sets are few. Cleared if it ever grows past
    // the bound (degenerate caller) rather than evicting per-entry.
    private static final int MAX_CACHED_AUTH_SETS = 1024;
    private static final ConcurrentHashMap<String, AccessEvaluator> EVALUATORS =
        new ConcurrentHashMap<>();

    private GeoMesaSecurityFunctions() {}

    /**
     * True if the auths satisfy the visibility expression; NULL/empty visibility
     * is unrestricted and invalid expressions are hidden (fail-closed).
     *
     * @param visibility the row's visibility expression (NULL/empty is unrestricted)
     * @param auths comma-delimited authorization tokens held by the caller
     * @return true if the auths satisfy the visibility expression
     */
    @ScalarFunction("is_visible")
    @Description("True if the comma-delimited auths satisfy the geomesa-security-style "
        + "visibility expression; NULL/empty visibility is unrestricted")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isVisible(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice visibility,
            @SqlType(StandardTypes.VARCHAR) Slice auths) {
        if (visibility == null || visibility.length() == 0) {
            return true;
        }
        try {
            return evaluator(auths.toStringUtf8()).canAccess(visibility.toStringUtf8());
        } catch (RuntimeException e) {
            return false;  // fail closed on invalid expressions
        }
    }

    private static AccessEvaluator evaluator(String auths) {
        if (EVALUATORS.size() > MAX_CACHED_AUTH_SETS) {
            EVALUATORS.clear();
        }
        return EVALUATORS.computeIfAbsent(auths, a -> {
            List<String> list = a.isEmpty() ? List.of() : Arrays.asList(a.split(","));
            return AccessEvaluator.of(Authorizations.of(list));
        });
    }
}
