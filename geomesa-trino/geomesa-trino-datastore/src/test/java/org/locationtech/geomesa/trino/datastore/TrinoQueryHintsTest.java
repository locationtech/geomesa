/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.data.Query;
import org.geotools.util.factory.Hints;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.locationtech.geomesa.index.geoserver.ViewParams;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TrinoQueryHintsTest {

    @AfterEach
    void clearPropertyOverride() {
        TrinoFeatureSource.CLIENT_VARIANT_PUSHDOWN.threadLocalValue().remove();
    }

    @Test
    void absentHintUsesProperty() {
        Query query = new Query("t");
        for (boolean enabled : new boolean[]{false, true}) {
            TrinoFeatureSource.CLIENT_VARIANT_PUSHDOWN.threadLocalValue().set(Boolean.toString(enabled));
            assertThat(TrinoFeatureSource.variantPrefilterEnabled(query)).isEqualTo(enabled);
            assertThat(query.getHints()).doesNotContainKey(QueryHints.TRINO_VARIANT_PREFILTER());
        }
    }

    @Test
    void explicitHintOverridesPropertyWithoutAffectingOtherQueries() {
        for (boolean enabled : new boolean[]{false, true}) {
            TrinoFeatureSource.CLIENT_VARIANT_PUSHDOWN.threadLocalValue().set(Boolean.toString(!enabled));
            Query query = new Query("t");
            query.getHints().put(QueryHints.TRINO_VARIANT_PREFILTER(), enabled);
            assertThat(TrinoFeatureSource.variantPrefilterEnabled(query)).isEqualTo(enabled);
            assertThat(TrinoFeatureSource.variantPrefilterEnabled(new Query("t"))).isEqualTo(!enabled);
            assertThat(TrinoFeatureSource.variantPrefilterEnabled(new Query(query))).isEqualTo(enabled);
        }
    }

    @Test
    void viewParamsOverridePropertyButYieldToExplicitHint() {
        for (boolean enabled : new boolean[]{false, true}) {
            TrinoFeatureSource.CLIENT_VARIANT_PUSHDOWN.threadLocalValue().set(Boolean.toString(!enabled));
            Query query = new Query("t");
            query.getHints().put(Hints.VIRTUAL_TABLE_PARAMETERS,
                Map.of("TRINO_VARIANT_PREFILTER", Boolean.toString(enabled)));
            assertThat(TrinoFeatureSource.variantPrefilterEnabled(query)).isEqualTo(enabled);
            query.getHints().put(QueryHints.TRINO_VARIANT_PREFILTER(), !enabled);
            assertThat(TrinoFeatureSource.variantPrefilterEnabled(query)).isEqualTo(!enabled);
        }
    }

    @Test
    void invalidViewParamFallsBackToProperty() {
        TrinoFeatureSource.CLIENT_VARIANT_PUSHDOWN.threadLocalValue().set("true");
        Query query = new Query("t");
        query.getHints().put(Hints.VIRTUAL_TABLE_PARAMETERS, Map.of("TRINO_VARIANT_PREFILTER", "invalid"));
        assertThat(TrinoFeatureSource.variantPrefilterEnabled(query)).isTrue();
        assertThat(query.getHints()).doesNotContainKey(QueryHints.TRINO_VARIANT_PREFILTER());
    }

    @Test
    void hintSurvivesSerialization() {
        Query query = new Query("t");
        query.getHints().put(QueryHints.TRINO_VARIANT_PREFILTER(), Boolean.FALSE);
        Hints restored = ViewParams.deserialize(ViewParams.serialize(query.getHints()));
        assertThat(restored.get(QueryHints.TRINO_VARIANT_PREFILTER())).isEqualTo(Boolean.FALSE);
    }
}
