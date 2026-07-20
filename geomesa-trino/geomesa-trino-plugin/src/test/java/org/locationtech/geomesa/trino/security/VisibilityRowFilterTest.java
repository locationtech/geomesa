/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the exact {@code is_visible(...)} SQL that {@link VisibilityRowFilter#conjunct} emits.
 * The datastore's {@code TrinoFeatureSource.visibilityConjunct} (guarded by
 * {@code TrinoFeatureSourceVisibilityTest}) MUST produce byte-identical output — the two are
 * intentionally duplicated across isolated modules. If you change the format here, change it
 * there too, or the two paths drift.
 */
class VisibilityRowFilterTest {

    @Test
    void conjunctQuotesColumnAndEscapesAuths() {
        assertThat(VisibilityRowFilter.conjunct("__vis__", List.of("basic", "privileged")))
            .isEqualTo("is_visible(\"__vis__\", 'basic,privileged')");
    }

    @Test
    void emptyAuthsEmitEmptyLiteral() {
        assertThat(VisibilityRowFilter.conjunct("__vis__", List.of()))
            .isEqualTo("is_visible(\"__vis__\", '')");
    }

    @Test
    void singleQuoteInAuthIsDoubled() {
        assertThat(VisibilityRowFilter.conjunct("__vis__", List.of("basic", "o'brien")))
            .isEqualTo("is_visible(\"__vis__\", 'basic,o''brien')");
    }

    @Test
    void doubleQuoteInColumnIsDoubled() {
        assertThat(VisibilityRowFilter.conjunct("ve\"rt", List.of("basic")))
            .isEqualTo("is_visible(\"ve\"\"rt\", 'basic')");
    }

    @Test
    void authTokenContainingCommaIsRejected() {
        for (String bad : List.of("FOO,BAR", "")) {
            assertThatThrownBy(() -> VisibilityRowFilter.conjunct("__vis__", List.of("basic", bad)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authorization token");
        }
    }

    @Test
    void nonCommaSpecialCharsAreAllowed() {
        assertThat(VisibilityRowFilter.conjunct("__vis__", List.of("FOO;BAR", "FOO:BAR", "FOO BAR")))
            .isEqualTo("is_visible(\"__vis__\", 'FOO;BAR,FOO:BAR,FOO BAR')");
    }
}
