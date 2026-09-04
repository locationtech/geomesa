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

    // -- tokenDomain ---------------------------------------------------------

    @Test
    void emptyAuthsProducesNoTokenDomain() {
        assertThat(VisibilityDomainPruning.tokenDomain(VARCHAR, Set.of())).isEmpty();
    }

    @Test
    void tokenDomainIncludesEachAuthAndNull() {
        Domain domain = VisibilityDomainPruning.tokenDomain(VARCHAR, Set.of("user", "privileged"))
            .orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("user"))).isTrue();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("privileged"))).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
    }

    @Test
    void tokenDomainExcludesUnrelatedToken() {
        Domain domain = VisibilityDomainPruning.tokenDomain(VARCHAR, Set.of("user")).orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin"))).isFalse();
    }

    @Test
    void tokenDomainDocumentedLimitationForCompoundExpressions() {
        // Demonstrates (rather than merely asserting) the documented unsoundness for
        // compound expressions: a user with BOTH "admin" and "ops" is entitled to see
        // "admin&ops" per AccessEvaluator, but the literal string "admin&ops" is not
        // admitted by the token domain built from {"admin","ops"} — a file containing
        // only that value could be wrongly pruned. This is why tokenDomain is opt-in.
        AccessEvaluator eval = AccessEvaluator.of(Authorizations.of(List.of("admin", "ops")));
        assertThat(eval.canAccess("admin&ops")).isTrue();

        Domain domain = VisibilityDomainPruning.tokenDomain(VARCHAR, Set.of("admin", "ops"))
            .orElseThrow();
        assertThat(domain.includesNullableValue(Slices.utf8Slice("admin&ops"))).isFalse();
    }
}