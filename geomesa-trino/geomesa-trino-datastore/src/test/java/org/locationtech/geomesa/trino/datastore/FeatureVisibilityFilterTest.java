/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.security.SecurityUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FeatureVisibilityFilterTest {

    private static SimpleFeature feature(String vis) throws Exception {
        SimpleFeatureType sft = DataUtilities.createType("t", "name:String");
        SimpleFeature f = new SimpleFeatureBuilder(sft).buildFeature("1", new Object[]{"x"});
        if (vis != null) {
            SecurityUtils.setFeatureVisibility(f, vis);
        }
        return f;
    }

    @Test
    void unrestrictedFeatureIsVisible() throws Exception {
        FeatureVisibilityFilter filter = new FeatureVisibilityFilter(List.of("admin"));
        assertThat(filter.test(feature(null))).isTrue();
        assertThat(filter.test(feature(""))).isTrue();
    }

    @Test
    void expressionEvaluatedAgainstAuths() throws Exception {
        FeatureVisibilityFilter filter = new FeatureVisibilityFilter(List.of("user"));
        assertThat(filter.test(feature("admin|user"))).isTrue();
        assertThat(filter.test(feature("admin&user"))).isFalse();
    }

    @Test
    void invalidExpressionFailsClosed() throws Exception {
        FeatureVisibilityFilter filter = new FeatureVisibilityFilter(List.of("admin"));
        assertThat(filter.test(feature("admin&&("))).isFalse();
    }

    @Test
    void emptyAuthsSeeOnlyUnrestricted() throws Exception {
        FeatureVisibilityFilter filter = new FeatureVisibilityFilter(List.of());
        assertThat(filter.test(feature(null))).isTrue();
        assertThat(filter.test(feature(""))).isTrue();
        assertThat(filter.test(feature("admin"))).isFalse();
    }
}
