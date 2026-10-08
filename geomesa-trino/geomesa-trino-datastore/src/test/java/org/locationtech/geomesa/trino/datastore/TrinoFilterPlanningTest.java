/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.data.DataUtilities;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.filter.function.CurrentDateFunction;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.locationtech.geomesa.trino.datastore.TrinoFeatureSource.ClientSideFiltering.*;

class TrinoFilterPlanningTest {

    private final SimpleFeatureType schema = DataUtilities.createType("test",
        "name:String,age:Integer,dtg:Date,*geom:Point:srid=4326,props:String:json=true");

    TrinoFilterPlanningTest() throws Exception {
        schema.getDescriptor("props").getUserData().put("json", "true");
    }

    private TrinoFeatureSource.FilterSplit split(String cql, TrinoFeatureSource.ClientSideFiltering mode)
            throws Exception {
        return TrinoFeatureSource.splitFilter(ECQL.toFilter(cql), schema, false, mode);
    }

    @Test
    void unsupportedFunctionsBecomeResidualsWithTheirAttributes() throws Exception {
        var result = split("age > 18 AND strToLowerCase(name) = 'alice'", PARTIAL);
        assertThat(result.pushableSql()).contains("\"age\" > 18").doesNotContain("strToLowerCase");
        assertThat(result.residual()).isNotNull();
        assertThat(result.residualAttributes()).containsExactly("name");
        var feature = DataUtilities.createFeature(schema, "id=ALICE|21|2026-10-08T00:00:00Z|POINT (0 0)|{}");
        assertThat(result.residual().evaluate(feature)).isTrue();
    }

    @Test
    void clientSidePolicyStillControlsUnsupportedFunctions() throws Exception {
        String unsupported = "strToLowerCase(name) = 'alice'";
        assertThatThrownBy(() -> split(unsupported, NONE)).hasMessageContaining("disabled");
        assertThatThrownBy(() -> split(unsupported, PARTIAL)).hasMessageContaining("partial pushdown");
        assertThat(split(unsupported, ALL).pushableSql()).isNull();
        assertThatThrownBy(() -> split("age > 18 AND " + unsupported, NONE)).hasMessageContaining("disabled");
    }

    @Test
    void splitterHandlesNestedConjunctionsWithoutUnsafeOrOrNotPushdown() throws Exception {
        var result = split("age > 18 AND (name = 'alice' AND strToLowerCase(name) = 'alice')", PARTIAL);
        assertThat(result.pushableSql()).contains("\"age\" > 18", "\"name\" = 'alice'");
        assertThat(result.residual()).isEqualTo(ECQL.toFilter("strToLowerCase(name) = 'alice'"));
        for (String cql : new String[]{"age > 18 OR strToLowerCase(name) = 'alice'",
                "NOT (age > 18 AND strToLowerCase(name) = 'alice')"}) {
            assertThat(split(cql, ALL).pushableSql()).isNull();
            assertThat(split(cql, ALL).residual()).isNotNull();
        }
    }

    @Test
    void simplifiesConstantFunctionsAndLogicalFilters() throws Exception {
        var result = split("INCLUDE AND NOT (NOT (name = strToLowerCase('ALICE')))", NONE);
        assertThat(result.pushableSql()).isEqualTo("\"name\" = 'alice'");
        assertThat(result.residual()).isNull();
        assertThat(split("EXCLUDE AND strToLowerCase(name) = 'alice'", NONE).pushableSql()).isEqualTo("0 = 1");
        assertThatThrownBy(() -> split("INCLUDE AND strToLowerCase(name) = 'alice'", PARTIAL))
            .hasMessageContaining("partial pushdown");
    }

    @Test
    void constantCutoffsAreEvaluatedAgainForEachQuery() {
        var count = new AtomicInteger();
        var ff = CommonFactoryFinder.getFilterFactory();
        var currentDate = new CurrentDateFunction() {
            @Override
            public Object evaluate(Object ignored) {
                return new Date(count.incrementAndGet() * 1000L);
            }
        };
        Filter filter = ff.less(ff.property("dtg"), currentDate);
        assertThat(TrinoFeatureSource.splitFilter(filter, schema, false, NONE).pushableSql())
            .contains("1970-01-01 00:00:01 UTC");
        assertThat(TrinoFeatureSource.splitFilter(filter, schema, false, NONE).pushableSql())
            .contains("1970-01-01 00:00:02 UTC");
    }

    @Test
    void implicitFeatureFunctionsAreNotFoldedIntoConstants() throws Exception {
        for (String cql : new String[]{"proxyId() = 1", "z2(geom) = 'a'", "xz2(geom) = 'a'", "fastproperty(0) = 'alice'",
                "murmurHash(fastproperty(0)) = 1"}) {
            var result = split(cql, NONE);
            assertThat(result.residual()).isNull();
            assertThat(result.pushableSql()).doesNotContain("NULL");
            assertThat(result.pushableSql()).contains(cql.startsWith("proxyId") ? "__fid__"
                : cql.startsWith("z2") || cql.startsWith("xz2") ? "geom" : "name");
        }
    }

    @Test
    void supportedFunctionWithUnsupportedArgumentTypeIsResidual() throws Exception {
        var booleanSchema = DataUtilities.createType("bools", "flag:Boolean");
        var filter = ECQL.toFilter("murmurHash(flag) > 0");
        var result = TrinoFeatureSource.splitFilter(filter, booleanSchema, false, ALL);
        assertThat(result.pushableSql()).isNull();
        assertThat(result.residual()).isEqualTo(filter);
        assertThat(result.residualAttributes()).containsExactly("flag");
    }

    @Test
    void variantPrefilterRetainsItsResidualAndRespectsPolicy() throws Exception {
        var filter = ECQL.toFilter("\"$.props.color\" = 'blue'");
        var result = TrinoFeatureSource.splitFilter(filter, schema, true, PARTIAL);
        assertThat(result.pushableSql()).contains("VARIANT");
        assertThat(result.residual()).isNotNull();
        assertThat(result.residualAttributes()).containsExactly("props");
        assertThatThrownBy(() -> TrinoFeatureSource.splitFilter(filter, schema, true, NONE))
            .hasMessageContaining("disabled");
    }
    @Test
    void removesWholeWorldIntersectionPredicatesOnIndexedGeometry() throws Exception {
        String world = "POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))";
        for (String cql : new String[]{"BBOX(geom, -180, -90, 180, 90)",
                "INTERSECTS(geom, " + world + ")", "INTERSECTS(" + world + ", geom)"}) {
            assertThat(split(cql, NONE).pushableSql()).isNull();
            var result = split("(" + cql + ") AND age > 18", NONE);
            assertThat(result.pushableSql()).isEqualTo("\"age\" > 18");
            assertThat(result.residual()).isNull();
        }
        assertThat(split("NOT BBOX(geom, -180, -90, 180, 90)", NONE).pushableSql()).isEqualTo("0 = 1");
        assertThat(split("BBOX(geom, -10, -10, 10, 10)", NONE).pushableSql()).contains("ST_Intersects");
        for (String op : new String[]{"WITHIN", "CONTAINS", "OVERLAPS", "DISJOINT"}) {
            assertThat(split(op + "(geom, " + world + ")", NONE).pushableSql()).contains("ST_");
        }
    }

    @Test
    void wholeWorldOptimizationDoesNotApplyToSecondaryGeometry() throws Exception {
        var twoGeometries = DataUtilities.createType("two", "*geom:Point:srid=4326,other:Point:srid=4326");
        var result = TrinoFeatureSource.splitFilter(ECQL.toFilter("BBOX(other, -180, -90, 180, 90)"),
            twoGeometries, false, NONE);
        assertThat(result.pushableSql()).contains("ST_Intersects", "other");
    }

    @Test
    void residualsRetainImplicitGeometryAndIndexedAttributeDependencies() throws Exception {
        var result = split("strToLowerCase(z2(geom)) = 'a'", ALL);
        assertThat(result.pushableSql()).isNull();
        assertThat(result.residualAttributes()).containsExactly("geom");
        var indexed = split("strToLowerCase(fastproperty(0)) = 'alice'", ALL);
        assertThat(indexed.residualAttributes()).containsExactlyInAnyOrder("name", "age", "dtg", "geom", "props");
    }

    @Test
    void constantHashFunctionsAreFolded() throws Exception {
        for (String function : new String[]{"murmurHash('alice')", "bucketHash('alice', 8)"}) {
            int value = (Integer) ECQL.toExpression(function).evaluate(null);
            var matching = split(function + " = " + value, NONE);
            assertThat(matching.pushableSql()).isNull();
            assertThat(matching.residual()).isNull();
            var different = split(function + " = " + (value + 1), NONE);
            assertThat(different.pushableSql()).isEqualTo("0 = 1");
            assertThat(different.residual()).isNull();
        }
    }

    @Test
    void baseEncoderInFunctionSupportIsRetained() {
        var ff = CommonFactoryFinder.getFilterFactory();
        var in = ff.function("in", ff.property("name"), ff.literal("alice"), ff.literal("bob"));
        var result = TrinoFeatureSource.splitFilter(ff.equal(in, ff.literal(true), true), schema, false, NONE);
        assertThat(result.pushableSql()).contains("IN", "'alice'", "'bob'");
        assertThat(result.residual()).isNull();
    }

}
