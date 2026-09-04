/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Builds {@code Domain} constraints on the visibility column that Iceberg's own
 * per-file manifest statistics (min/max, null-count) can use to prune whole
 * files before any row is read — a necessary-condition pre-filter that runs
 * ALONGSIDE, and never in place of, the always-enforced {@code is_visible()}
 * row filter installed by {@link VisibilityAccessControl}.
 *
 * <p><strong>Direction of error.</strong> A domain here only narrows the set of
 * candidate files; it can never widen it. So a wrong or overly-narrow domain
 * can only make a query wrongly omit rows the caller was entitled to see (a
 * correctness/availability bug) — it can never cause a row the row filter would
 * have hidden to be returned (never a security/leak bug). That asymmetry is
 * what bounds the blast radius of an incomplete {@link #expressionDomain}
 * candidate universe (see below) to a correctness bug rather than a leak.
 *
 * <p><strong>Which way "wrong" cuts for {@link #expressionDomain}.</strong> Its
 * declared candidate universe must be COMPLETE to be sound. Over-declaring is
 * harmless: a declared value that never occurs simply never matches, so pruning
 * is merely weaker. Under-declaring is the dangerous direction: because the
 * domain is a positive IN-list, a value that actually occurs but is omitted is
 * never admitted, so a file holding only that value is over-pruned — a caller
 * whose auths actually admit it silently loses those rows (a correctness/
 * availability bug). Still never a leak, per the invariant above; but an
 * incomplete universe prunes too MUCH, not too little.
 */
public final class VisibilityDomainPruning {

    private VisibilityDomainPruning() {}

    /**
     * Sound unconditionally, for any visibility expression grammar: an identity
     * with no authorizations can only ever be granted access to a NULL/empty
     * visibility (see {@link GeoMesaSecurityFunctions} — {@code AccessEvaluator}
     * never grants a non-empty expression to an empty auth set, no matter how
     * simple or compound that expression is). So when {@code auths} is empty,
     * the only rows the caller could ever see are NULL, and Iceberg can prune
     * any file whose null-count for the column is zero.
     *
     * @param visColumnType the visibility column's type (always VARCHAR)
     * @param auths the resolved authorizations for the querying identity
     * @return a NULL-only domain when {@code auths} is empty; empty otherwise
     *         (an empty result is not a signal to fall back — see
     *         {@link #expressionDomain} for the non-empty-auths case)
     */
    public static Optional<Domain> emptyAuthsDomain(VarcharType visColumnType, Set<String> auths) {
        if (!auths.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Domain.onlyNull(visColumnType));
    }

    /**
     * Sound for ANY visibility expression grammar, compound included: this prunes
     * on literal expression VALUES via the real {@code is_visible()} decision
     * rather than on decomposed tokens. {@code candidateExpressions} is the closed universe of
     * every distinct non-null value the visibility column can ever hold (e.g. a
     * declared clearance ladder: {@code "U"}, {@code "U&FOUO"}, ...); each one is
     * run through the real {@link GeoMesaSecurityFunctions#isVisible} decision —
     * the identical engine the row filter uses — so "does the caller's auth set
     * admit this literal string" is answered exactly, with no token-vs-expression
     * mismatch for {@code &}/{@code |} to fall through.
     *
     * <p>Soundness therefore reduces to completeness of {@code
     * candidateExpressions}, and completeness is REQUIRED, not best-effort: a
     * value present in the column but omitted here is never added to the admitted
     * set, so a file holding only that value is pruned even for a caller whose
     * auths would admit it — silently dropping rows they were entitled to (a
     * correctness/availability bug). This never leaks (the domain only narrows the
     * file set; the {@code is_visible()} row filter still runs), but under-declaring
     * prunes too MUCH, not too little. Over-declaring is safe: a declared value that
     * never occurs simply never matches. Scales to tens-to-low-hundreds of distinct
     * values; not intended for effectively-unique per-row visibility strings.
     *
     * @param visColumnType the visibility column's type (always VARCHAR)
     * @param candidateExpressions every distinct non-null value the column can hold
     * @param auths the resolved authorizations for the querying identity
     * @return an IN-list-plus-null domain over the expressions {@code auths} can
     *         access; empty when {@code auths} or {@code candidateExpressions} is
     *         empty (use {@link #emptyAuthsDomain} for the former)
     */
    public static Optional<Domain> expressionDomain(VarcharType visColumnType,
                                                      Set<String> candidateExpressions,
                                                      Set<String> auths) {
        if (auths.isEmpty() || candidateExpressions.isEmpty()) {
            return Optional.empty();
        }
        String authsCsv = String.join(",", new ArrayList<>(new TreeSet<>(auths)));
        List<Range> ranges = candidateExpressions.stream()
            .filter(expr -> GeoMesaSecurityFunctions.isVisible(
                Slices.utf8Slice(expr), Slices.utf8Slice(authsCsv)))
            .map(expr -> Range.equal(visColumnType, Slices.utf8Slice(expr)))
            .toList();
        if (ranges.isEmpty()) {
            // None of the declared expressions are visible: only NULL is admissible,
            // same as the empty-auths case.
            return Optional.of(Domain.onlyNull(visColumnType));
        }
        return Optional.of(Domain.create(SortedRangeSet.copyOf(visColumnType, ranges), true));
    }
}
