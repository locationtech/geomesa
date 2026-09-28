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
    void emptyAuthsProducesUnrestrictedDomain() {
        // A no-auth caller can still see the unrestricted rows, which is_visible() returns
        // for both NULL and the empty string "" — so the domain admits both, and is NOT
        // only-null (the "" fix: files holding only "" must survive pruning too).
        Optional<Domain> domain = VisibilityDomainPruning.emptyAuthsDomain(VARCHAR, Set.of());
        assertThat(domain).isPresent();
        assertThat(domain.get().isOnlyNull()).isFalse();
        assertThat(domain.get().includesNullableValue(null)).isTrue();
        assertThat(domain.get().includesNullableValue(Slices.utf8Slice(""))).isTrue();
    }

    @Test
    void nonEmptyAuthsProducesNoEmptyAuthsDomain() {
        assertThat(VisibilityDomainPruning.emptyAuthsDomain(VARCHAR, Set.of("user"))).isEmpty();
    }

    @Test
    void emptyAuthsDomainAdmitsOnlyUnrestrictedValues() {
        // The domain admits the unrestricted values (NULL and "") and no concrete expression.
        Domain domain = VisibilityDomainPruning.emptyAuthsDomain(VARCHAR, Set.of()).orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin"))).isFalse();
        assertThat(domain.includesNullableValue(Slices.utf8Slice(""))).isTrue();
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
        // Unrestricted values are always admitted, regardless of the caller's auths.
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice(""))).isTrue();
    }

    @Test
    void expressionDomainOnlyIncludesCandidatesActuallyVisible() {
        Domain domain = VisibilityDomainPruning.expressionDomain(
            VARCHAR, Set.of("basic", "basic&privileged", "admin"), Set.of("basic", "privileged")).orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("basic"))).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("basic&privileged"))).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin"))).isFalse();
        // Unrestricted values (NULL and "") ride along with the admissible expressions.
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice(""))).isTrue();
    }

    @Test
    void expressionDomainFallsBackToUnrestrictedWhenNoCandidateIsVisible() {
        // No declared candidate is admissible, so the domain collapses to the unrestricted-only
        // set (NULL and ""), identical to emptyAuthsDomain — never only-null, so files holding
        // only "" survive.
        Domain domain = VisibilityDomainPruning
            .expressionDomain(VARCHAR, Set.of("privileged"), Set.of("basic"))
            .orElseThrow();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice(""))).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("privileged"))).isFalse();
    }
}
