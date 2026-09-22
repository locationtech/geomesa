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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Arrays;
import java.util.List;

/**
 * Row-entitlement UDF. A row is visible only when it carries a real visibility
 * expression that the caller's auths satisfy: a NULL or empty-string
 * visibility is treated as an anomaly (a row with no real expression) and is
 * hidden from everyone, and invalid expressions are hidden too (fail-closed).
 *
 * <p><strong>Intentional divergence from geomesa-security.</strong> The native
 * geomesa-security {@code VisibilityUtils} path treats NULL/empty as
 * unrestricted (visible to all). This Trino-layer UDF is deliberately stricter —
 * it fails closed on anomalous NULL/empty values — so that a row lacking any
 * real visibility expression is never surfaced through Trino. Both Trino
 * enforcement paths resolve to this single UDF (the plugin row filter and the
 * datastore's {@code is_visible(...)} pushdown), so the stricter rule applies
 * uniformly across Trino.
 */
public final class GeoMesaSecurityFunctions {

    private record Decision(String auths, String visibility) {}

    // Two bounded worker-lifetime caches (Guava — already on the plugin
    // classpath via trino-iceberg, and what Trino itself caches with), layered:
    // (a) auths → AccessEvaluator, so a caller's auth set is parsed once;
    // (b) (auths, visibility) → decision. The visibility row filter invokes
    //     is_visible once per row, and AccessEvaluator.canAccess(String)
    //     re-parses the expression on every call — while distinct
    //     (auth-set, expression) pairs are few (users × the dataset's
    //     visibility ladder). Caching the boolean makes the steady-state
    //     per-row cost a single cache hit. Sound because the decision is a
    //     pure function of the pair; fail-closed results for invalid
    //     expressions are cached the same way (equally deterministic).
    private static final LoadingCache<String, AccessEvaluator> EVALUATORS =
        CacheBuilder.newBuilder().maximumSize(1024)
            .build(CacheLoader.from(GeoMesaSecurityFunctions::buildEvaluator));
    private static final LoadingCache<Decision, Boolean> DECISIONS =
        CacheBuilder.newBuilder().maximumSize(8192)
            .build(CacheLoader.from(GeoMesaSecurityFunctions::evaluate));

    private GeoMesaSecurityFunctions() {}

    /**
     * True if the auths satisfy the visibility expression. A NULL or empty
     * visibility has no real expression and is hidden from everyone (fail-closed),
     * as are invalid expressions.
     *
     * @param visibility the row's visibility expression (NULL/empty is hidden)
     * @param auths comma-delimited authorization tokens held by the caller
     * @return true only if the visibility is a real expression the auths satisfy
     */
    @ScalarFunction("is_visible")
    @Description("True if the comma-delimited auths satisfy the geomesa-security-style "
        + "visibility expression; NULL/empty visibility is hidden from everyone")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isVisible(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice visibility,
            @SqlType(StandardTypes.VARCHAR) Slice auths) {
        if (visibility == null || visibility.length() == 0) {
            return false;  // no real visibility expression -> anomaly, hidden from all
        }
        return DECISIONS.getUnchecked(
            new Decision(auths.toStringUtf8(), visibility.toStringUtf8()));
    }

    private static Boolean evaluate(Decision decision) {
        try {
            // getUnchecked rewraps loader failures as UncheckedExecutionException
            // (a RuntimeException), so an invalid auth string fails closed too.
            return EVALUATORS.getUnchecked(decision.auths()).canAccess(decision.visibility());
        } catch (RuntimeException e) {
            return Boolean.FALSE;  // fail closed on invalid expressions
        }
    }

    private static AccessEvaluator buildEvaluator(String auths) {
        List<String> list = auths.isEmpty() ? List.of() : Arrays.asList(auths.split(","));
        return AccessEvaluator.of(Authorizations.of(list));
    }
}

