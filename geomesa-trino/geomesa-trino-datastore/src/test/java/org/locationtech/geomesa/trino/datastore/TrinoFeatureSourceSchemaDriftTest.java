/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.DataUtilities;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@link TrinoFeatureSource#schemaDrifted}: the predicate that decides
 * whether a failed query should invalidate the cached schema and retry —
 * e.g. after a table is recreated without its visibility column while the
 * long-lived datastore still holds the old discovery.
 */
class TrinoFeatureSourceSchemaDriftTest {

    private static SimpleFeatureType sft(String spec, String visColumn) throws Exception {
        SimpleFeatureType sft = DataUtilities.createType("t", spec);
        if (visColumn != null) {
            sft.getUserData().put(TrinoSchemaDiscovery.VIS_COLUMN_KEY, visColumn);
        }
        return sft;
    }

    @Test
    void identicalSchemasAreNotDrifted() throws Exception {
        assertThat(TrinoFeatureSource.schemaDrifted(
            sft("name:String,age:Integer", "__vis__"),
            sft("name:String,age:Integer", "__vis__"))).isFalse();
        assertThat(TrinoFeatureSource.schemaDrifted(
            sft("name:String", null),
            sft("name:String", null))).isFalse();
    }

    @Test
    void removedVisibilityColumnIsDrift() throws Exception {
        assertThat(TrinoFeatureSource.schemaDrifted(
            sft("name:String", "__vis__"),
            sft("name:String", null))).isTrue();
    }

    @Test
    void addedVisibilityColumnIsDrift() throws Exception {
        assertThat(TrinoFeatureSource.schemaDrifted(
            sft("name:String", null),
            sft("name:String", "__vis__"))).isTrue();
    }

    @Test
    void changedAttributesAreDrift() throws Exception {
        assertThat(TrinoFeatureSource.schemaDrifted(
            sft("name:String,age:Integer", null),
            sft("name:String", null))).isTrue();
        // Same names, different binding.
        assertThat(TrinoFeatureSource.schemaDrifted(
            sft("name:String,age:Integer", null),
            sft("name:String,age:String", null))).isTrue();
    }
}
