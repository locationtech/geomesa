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

import java.util.List;
import java.util.Optional;
import java.util.Set;

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
 * deployment before it can ship.
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
}