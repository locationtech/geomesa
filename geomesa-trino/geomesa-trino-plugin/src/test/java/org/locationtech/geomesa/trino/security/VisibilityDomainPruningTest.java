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
import io.trino.spi.type.VarcharType;
import org.apache.accumulo.access.AccessEvaluator;
import org.apache.accumulo.access.Authorizations;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class VisibilityDomainPruningTest {

    private static final VarcharType VARCHAR = VarcharType.VARCHAR;

    // -- emptyAuthsDomain --------------------------------------------------

    @Test
    void emptyAuthsProducesNoneDomain() {
        // No auths -> no rows visible at all (no satisfiable expression, and NULL/empty
        // are anomalies hidden from everyone) -> Domain.none prunes every file.
        Optional<Domain> domain = VisibilityDomainPruning.emptyAuthsDomain(VARCHAR, Set.of());
        assertThat(domain).isPresent();
        assertThat(domain.get().isNone()).isTrue();
    }

    @Test
    void nonEmptyAuthsProducesNoEmptyAuthsDomain() {
        assertThat(VisibilityDomainPruning.emptyAuthsDomain(VARCHAR, Set.of("user"))).isEmpty();
    }

    @Test
    void emptyAuthsDomainAdmitsNothing() {
        // Sanity: the domain admits no value at all — not a concrete string, not the
        // empty string, and not NULL (NULL/empty are hidden anomalies).
        Domain domain = VisibilityDomainPruning.emptyAuthsDomain(VARCHAR, Set.of()).orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin"))).isFalse();
        assertThat(domain.includesNullableValue(Slices.utf8Slice(""))).isFalse();
        assertThat(domain.includesNullableValue(null)).isFalse();
    }

    @Test
    void emptyAuthsDomainConsistentWithAccessEvaluatorForAnyExpression() {
        // Cross-check against the real AccessEvaluator (same engine GeoMesaSecurityFunctions
        // uses): with no authorizations, no real expression is accessible, regardless of
        // expression complexity — and NULL/empty are hidden separately. Together these make
        // the empty-auths tier a sound "prune everything" for a no-auth caller.
        AccessEvaluator eval = AccessEvaluator.of(Authorizations.of(List.of()));
        String[] expressions = {"admin", "admin&ops", "admin|ops", "(admin|ops)&secure"};
        for (String expr : expressions) {
            assertThat(eval.canAccess(expr)).isFalse();
        }
    }

    @Test
    void expressionDomainEmptyAuthsOrEmptyCandidatesYieldsNoDomain() {
        assertThat(VisibilityDomainPruning.expressionDomain(VARCHAR, Set.of("basic"), Set.of()))
            .isEmpty();
        assertThat(VisibilityDomainPruning.expressionDomain(VARCHAR, Set.of(), Set.of("basic")))
            .isEmpty();
    }

    @Test
    void expressionDomainAdmitsCompoundExpressionCallerCanSatisfy() {
        // A caller holding both "admin" and "ops" IS entitled to the compound value
        // "admin&ops" per AccessEvaluator, and expressionDomain admits it: it checks the
        // literal candidate expression through the real is_visible() decision rather than
        // decomposing into tokens, so compound (&/|) values are handled correctly.
        AccessEvaluator eval = AccessEvaluator.of(Authorizations.of(List.of("admin", "ops")));
        assertThat(eval.canAccess("admin&ops")).isTrue();

        Domain domain = VisibilityDomainPruning
            .expressionDomain(VARCHAR, Set.of("admin&ops"), Set.of("admin", "ops"))
            .orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin&ops"))).isTrue();
        // NULL is never admitted: an anomalous NULL visibility is hidden from everyone.
        assertThat(domain.includesNullableValue(null)).isFalse();
    }

    @Test
    void expressionDomainExcludesCompoundExpressionCallerCannotSatisfy() {
        // Only "admin&ops" is declared and this caller cannot satisfy it, so no candidate
        // is visible -> Domain.none (nothing admitted, not even NULL).
        Domain domain = VisibilityDomainPruning
            .expressionDomain(VARCHAR, Set.of("admin&ops"), Set.of("admin"))
            .orElseThrow();
        assertThat(domain.isNone()).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin&ops"))).isFalse();
        assertThat(domain.includesNullableValue(null)).isFalse();
    }

    @Test
    void expressionDomainOnlyIncludesCandidatesActuallyVisible() {
        Domain domain = VisibilityDomainPruning.expressionDomain(
            VARCHAR, Set.of("basic", "basic&privileged", "admin"), Set.of("basic", "privileged")).orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("basic"))).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("basic&privileged"))).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin"))).isFalse();
        // NULL is never admitted.
        assertThat(domain.includesNullableValue(null)).isFalse();
    }

    @Test
    void expressionDomainFallsBackToNoneWhenNoCandidateIsVisible() {
        Domain domain = VisibilityDomainPruning
            .expressionDomain(VARCHAR, Set.of("privileged"), Set.of("basic"))
            .orElseThrow();
        assertThat(domain.isNone()).isTrue();
    }

    @Test
    void expressionDomainNeverAdmitsEmptyStringEvenIfDeclared() {
        // An empty candidate string is a NULL/empty anomaly and must never be admitted,
        // even if an operator mistakenly declares "" in the universe.
        Domain domain = VisibilityDomainPruning
            .expressionDomain(VARCHAR, Set.of("basic", ""), Set.of("basic"))
            .orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("basic"))).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice(""))).isFalse();
        assertThat(domain.includesNullableValue(null)).isFalse();
    }
}
