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
 * what makes the token-based tier below safe to offer as an explicit,
 * documented opt-in rather than something that must be proven sound for every
 * deployment before it can ship — and it's also why an incomplete {@link
 * #expressionDomain} candidate set degrades to "prunes less" rather than
 * "prunes incorrectly."
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
     *         {@link #tokenDomain} for the opt-in non-empty-auths case)
     */
    public static Optional<Domain> emptyAuthsDomain(VarcharType visColumnType, Set<String> auths) {
        if (!auths.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Domain.onlyNull(visColumnType));
    }

    /**
     * Opt-in: builds an IN-list domain of the caller's individual auth tokens
     * plus NULL. <strong>Not sound for compound visibility expressions.</strong>
     * Correct only when every value ever stored in the visibility column is a
     * single literal token (no {@code &}, no {@code |}) — e.g. a coarse
     * classification ladder such as {@code "public"}/{@code "internal"}/
     * {@code "secret"}. A file holding a compound expression such as
     * {@code "admin&ops"} can be wrongly pruned even though a caller with both
     * tokens is entitled to it: the string {@code "admin&ops"} doesn't equal
     * either injected token and may fall outside the file's [min, max] range
     * for them. Per the class javadoc this can only hide rows, never leak them
     * — but callers must still gate this behind an explicit, documented
     * configuration flag rather than enabling it unconditionally.
     *
     * @param visColumnType the visibility column's type (always VARCHAR)
     * @param auths the resolved authorizations for the querying identity
     * @return an IN-list-plus-null domain over the given tokens; empty when
     *         {@code auths} is empty (use {@link #emptyAuthsDomain} for that case)
     */
    public static Optional<Domain> tokenDomain(VarcharType visColumnType, Set<String> auths) {
        if (auths.isEmpty()) {
            return Optional.empty();
        }
        List<Range> ranges = auths.stream()
            .map(token -> Range.equal(visColumnType, Slices.utf8Slice(token)))
            .toList();
        return Optional.of(Domain.create(SortedRangeSet.copyOf(visColumnType, ranges), true));
    }

    /**
     * Sound for ANY visibility expression grammar, compound included — unlike
     * {@link #tokenDomain}, this prunes on literal expression VALUES rather than
     * decomposed tokens. {@code candidateExpressions} is the closed universe of
     * every distinct non-null value the visibility column can ever hold (e.g. a
     * declared clearance ladder: {@code "U"}, {@code "U&FOUO"}, ...); each one is
     * run through the real {@link GeoMesaSecurityFunctions#isVisible} decision —
     * the identical engine the row filter uses — so "does the caller's auth set
     * admit this literal string" is answered exactly, with no token-vs-expression
     * mismatch for {@code &}/{@code |} to fall through.
     *
     * <p>Soundness therefore reduces to completeness of {@code
     * candidateExpressions}: an omitted value just makes its files un-prunable
     * (same narrows-never-widens direction as the rest of this class), never a
     * leak. Scales to tens-to-low-hundreds of distinct values; not intended for
     * effectively-unique per-row visibility strings.
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