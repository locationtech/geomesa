/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GeoMesaSecurityFunctionsTest {

    private static boolean visible(String vis, String auths) {
        Slice v = vis == null ? null : Slices.utf8Slice(vis);
        return GeoMesaSecurityFunctions.isVisible(v, Slices.utf8Slice(auths));
    }

    @Test
    void nullOrEmptyVisibilityIsUnrestricted() {
        assertThat(visible(null, "admin")).isTrue();
        assertThat(visible("", "admin")).isTrue();
        assertThat(visible(null, "")).isTrue();
    }

    @Test
    void singleAuthMatches() {
        assertThat(visible("admin", "admin")).isTrue();
        assertThat(visible("admin", "user")).isFalse();
    }

    @Test
    void andExpressionRequiresAllAuths() {
        assertThat(visible("admin&ops", "admin,ops")).isTrue();
        assertThat(visible("admin&ops", "admin")).isFalse();
    }

    @Test
    void orExpressionRequiresAnyAuth() {
        assertThat(visible("admin|user", "user")).isTrue();
        assertThat(visible("admin|user", "ops")).isFalse();
    }

    @Test
    void nestedExpression() {
        assertThat(visible("(admin|ops)&secure", "ops,secure")).isTrue();
        assertThat(visible("(admin|ops)&secure", "ops")).isFalse();
        assertThat(visible("(admin|ops)&secure", "secure")).isFalse();
    }

    @Test
    void emptyAuthsSeeOnlyUnrestrictedRows() {
        assertThat(visible(null, "")).isTrue();
        assertThat(visible("admin", "")).isFalse();
    }

    @Test
    void invalidExpressionFailsClosed() {
        assertThat(visible("admin&&(", "admin")).isFalse();
        assertThat(visible("admin&", "admin")).isFalse();
    }

    @Test
    void repeatedEvaluationsAreConsistent() {
        // Second and later calls for the same (auths, visibility) pair are served
        // from the decision cache; results must match the uncached first call —
        // including the cached fail-closed result for an invalid expression.
        for (int i = 0; i < 3; i++) {
            assertThat(visible("admin&ops", "admin,ops")).isTrue();
            assertThat(visible("admin&ops", "ops")).isFalse();
            assertThat(visible("admin&&(", "admin")).isFalse();
        }
    }

    @Test
    void cacheOverflowKeepsAnswersCorrect() {
        // Push well past one generation of both caches (distinct auth sets and
        // distinct decisions) to force rotations, then verify fresh and
        // previously-seen pairs still answer correctly.
        for (int i = 0; i < 20_000; i++) {
            assertThat(visible("tok" + i, "tok" + i)).isTrue();
        }
        assertThat(visible("admin", "admin")).isTrue();
        assertThat(visible("admin", "user")).isFalse();
        assertThat(visible("tok0", "tok0")).isTrue();
    }
}
