/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.security.SecurityUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class VisibilityFilteringFeatureReaderTest {

    private static final SimpleFeatureType SFT;
    static {
        try {
            SFT = DataUtilities.createType("t", "name:String");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static SimpleFeature feature(String id, String vis) {
        SimpleFeature f = new SimpleFeatureBuilder(SFT).buildFeature(id, new Object[]{id});
        if (vis != null) {
            SecurityUtils.setFeatureVisibility(f, vis);
        }
        return f;
    }

    private static FeatureReader<SimpleFeatureType, SimpleFeature> readerOf(SimpleFeature... features) {
        Iterator<SimpleFeature> it = List.of(features).iterator();
        return new FeatureReader<>() {
            @Override public SimpleFeatureType getFeatureType() { return SFT; }
            @Override public SimpleFeature next() { return it.next(); }
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public void close() {}
        };
    }

    @Test
    void dropsFeaturesTheAuthsCannotSee() throws Exception {
        var wrapped = new VisibilityFilteringFeatureReader(
            readerOf(feature("1", null), feature("2", "admin"), feature("3", "user")),
            new FeatureVisibilityFilter(List.of("user")));
        List<String> ids = new ArrayList<>();
        while (wrapped.hasNext()) {
            ids.add(wrapped.next().getID());
        }
        assertThat(ids).containsExactly("1", "3");
    }

    @Test
    void hasNextIsIdempotent() throws Exception {
        var wrapped = new VisibilityFilteringFeatureReader(
            readerOf(feature("1", "admin")),
            new FeatureVisibilityFilter(List.of()));
        assertThat(wrapped.hasNext()).isFalse();
        assertThat(wrapped.hasNext()).isFalse();
    }
}
