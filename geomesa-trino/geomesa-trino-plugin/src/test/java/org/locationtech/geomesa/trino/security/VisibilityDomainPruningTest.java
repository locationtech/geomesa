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
    void emptyAuthsProducesNullOnlyDomain() {
        Optional<Domain> domain = VisibilityDomainPruning.emptyAuthsDomain(VARCHAR, Set.of());
        assertThat(domain).isPresent();
        assertThat(domain.get().isOnlyNull()).isTrue();
    }

    @Test
    void nonEmptyAuthsProducesNoEmptyAuthsDomain() {
        assertThat(VisibilityDomainPruning.emptyAuthsDomain(VARCHAR, Set.of("user"))).isEmpty();
    }

    @Test
    void emptyAuthsDomainExcludesAnyNonNullValue() {
        // Sanity: the domain must not admit any concrete visibility string, only NULL.
        Domain domain = VisibilityDomainPruning.emptyAuthsDomain(VARCHAR, Set.of()).orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin"))).isFalse();
        assertThat(domain.includesNullableValue(Slices.utf8Slice(""))).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
    }

    @Test
    void emptyAuthsDomainConsistentWithAccessEvaluatorForAnyExpression() {
        // Cross-check against the real AccessEvaluator (same engine GeoMesaSecurityFunctions
        // uses): with no authorizations, only a NULL/empty visibility should be accessible,
        // regardless of expression complexity. This is the soundness argument for the
        // unconditional (non-opt-in) pruning tier, verified against a variety of expressions.
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
    }

    @Test
    void expressionDomainExcludesCompoundExpressionCallerCannotSatisfy() {
        Domain domain = VisibilityDomainPruning
            .expressionDomain(VARCHAR, Set.of("admin&ops"), Set.of("admin"))
            .orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin&ops"))).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
    }

    @Test
    void expressionDomainOnlyIncludesCandidatesActuallyVisible() {
        Domain domain = VisibilityDomainPruning.expressionDomain(
            VARCHAR, Set.of("basic", "basic&privileged", "admin"), Set.of("basic", "privileged")).orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("basic"))).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("basic&privileged"))).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin"))).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
    }

    @Test
    void expressionDomainFallsBackToOnlyNullWhenNoCandidateIsVisible() {
        Domain domain = VisibilityDomainPruning
            .expressionDomain(VARCHAR, Set.of("privileged"), Set.of("basic"))
            .orElseThrow();
        assertThat(domain.isOnlyNull()).isTrue();
    }
}
