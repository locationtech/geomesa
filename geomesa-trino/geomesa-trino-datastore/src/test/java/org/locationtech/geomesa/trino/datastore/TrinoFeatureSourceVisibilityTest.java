/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the exact {@code is_visible(...)} SQL that {@code TrinoFeatureSource.visibilityConjunct}
 * emits. The plugin's {@code VisibilityRowFilter.conjunct} (guarded by
 * {@code VisibilityRowFilterTest}) MUST produce byte-identical output — the two are intentionally
 * duplicated across isolated modules. If you change the format here, change it there too, or the
 * two paths drift.
 */
class TrinoFeatureSourceVisibilityTest {

    @Test
    void conjunctQuotesColumnAndEscapesAuths() {
        assertThat(TrinoFeatureSource.visibilityConjunct("__vis__", List.of("admin", "u'ser")))
            .isEqualTo("is_visible(\"__vis__\", 'admin,u''ser')");
    }

    @Test
    void emptyAuthsEmitEmptyLiteral() {
        assertThat(TrinoFeatureSource.visibilityConjunct("__vis__", List.of()))
            .isEqualTo("is_visible(\"__vis__\", '')");
    }

    @Test
    void combineBothWrapsFilterAndAnds() {
        assertThat(TrinoFeatureSource.combineWhere("a = 1", "vis()"))
            .isEqualTo(" WHERE (a = 1) AND vis()");
    }

    @Test
    void authTokenContainingCommaIsRejected() {
        for (String bad : List.of("FOO,BAR", "")) {
            assertThatThrownBy(() ->
                TrinoFeatureSource.visibilityConjunct("__vis__", List.of("basic", bad)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authorization token");
        }
    }

    @Test
    void nonCommaSpecialCharsAreAllowed() {
        assertThat(TrinoFeatureSource.visibilityConjunct("__vis__", List.of("FOO;BAR", "FOO:BAR", "FOO BAR")))
            .isEqualTo("is_visible(\"__vis__\", 'FOO;BAR,FOO:BAR,FOO BAR')");
    }

    @Test
    void combineHandlesAbsentParts() {
        assertThat(TrinoFeatureSource.combineWhere(null, null)).isEmpty();
        assertThat(TrinoFeatureSource.combineWhere("a = 1", null)).isEqualTo(" WHERE a = 1");
        assertThat(TrinoFeatureSource.combineWhere(null, "vis()")).isEqualTo(" WHERE vis()");
    }
}
