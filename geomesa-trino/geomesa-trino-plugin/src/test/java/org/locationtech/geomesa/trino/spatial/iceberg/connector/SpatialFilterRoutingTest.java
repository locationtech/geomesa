/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.airlift.slice.Slices;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.TestGeometryType;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class SpatialFilterRoutingTest {

    /** Builds an st_intersects call with a real JTS-serialized Geometry constant so that
     *  tryExtractEnvelope succeeds and collectSpatialMatches actually adds a match. */
    private static Call stIntersects(String geomCol, org.locationtech.jts.geom.Envelope env) {
        io.airlift.slice.Slice geomSlice = io.trino.geospatial.serde.JtsGeometrySerde.serialize(
            new GeometryFactory().toGeometry(env));
        Constant geomConst = new Constant(geomSlice, TestGeometryType.GEOMETRY);
        return new Call(
            io.trino.spi.type.BooleanType.BOOLEAN,
            new FunctionName("st_intersects"),
            List.of(
                new Variable(geomCol, VarbinaryType.VARBINARY),
                geomConst
            ));
    }

    /** Builds an arbitrary ST_* call with a real Geometry constant, mirroring {@link
     *  #stIntersects}. */
    private static Call stCall(String functionName, String geomCol,
                               org.locationtech.jts.geom.Envelope env) {
        io.airlift.slice.Slice geomSlice = io.trino.geospatial.serde.JtsGeometrySerde.serialize(
            new GeometryFactory().toGeometry(env));
        Constant geomConst = new Constant(geomSlice, TestGeometryType.GEOMETRY);
        return new Call(
            io.trino.spi.type.BooleanType.BOOLEAN,
            new FunctionName(functionName),
            List.of(new Variable(geomCol, VarbinaryType.VARBINARY), geomConst));
    }

    /** Builds an st_disjoint call with a real Geometry constant. Used to assert
     *  disjoint is NOT collected for pushdown. */
    private static Call stDisjoint(String geomCol, org.locationtech.jts.geom.Envelope env) {
        return stCall("st_disjoint", geomCol, env);
    }

    private static Call and(ConnectorExpression... args) {
        return new Call(
            io.trino.spi.type.BooleanType.BOOLEAN,
            AND_FUNCTION_NAME,
            List.of(args));
    }

    @Test
    void stDisjointIsNotCollectedForPushdown() {
        // ST_Disjoint matches rows that do NOT overlap the envelope, so the
        // overlap-only bbox/Z2 domains this connector injects would prune away
        // exactly the answer set. It must never produce a SpatialMatch — the
        // predicate falls through to the delegate and is evaluated row-by-row.
        SpatialConnectorMetadata m = new SpatialConnectorMetadata(null, null);
        org.locationtech.jts.geom.Envelope env = new org.locationtech.jts.geom.Envelope(0, 1, 0, 1);
        assertThat(m.findAllSpatialMatches(stDisjoint("geom", env))).isEmpty();
        // And an ANDed intersects+disjoint yields only the intersects match.
        List<SpatialConnectorMetadata.SpatialMatch> mixed = m.findAllSpatialMatches(
            and(stIntersects("center", env), stDisjoint("ellipse", env)));
        assertThat(mixed)
            .extracting(SpatialConnectorMetadata.SpatialMatch::geomName)
            .containsExactly("center");
    }

    @Test
    void spatialPredicatesUnderOrOrNotAreNotCollected() {
        // A spatial predicate under $or/$not is not a top-level constraint: pushing
        // its envelope would prune rows that satisfy the query through the other
        // branch (DISJOINT(a) OR CROSSES(b)) or that live outside it (NOT INTERSECTS).
        // Caught end-to-end by the datastore filter-parity suite.
        SpatialConnectorMetadata m = new SpatialConnectorMetadata(null, null);
        org.locationtech.jts.geom.Envelope env = new org.locationtech.jts.geom.Envelope(0, 1, 0, 1);
        Call or = new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME,
            List.of(stIntersects("center", env), stCall("st_crosses", "ellipse", env)));
        assertThat(m.findAllSpatialMatches(or)).isEmpty();
        Call not = new Call(BooleanType.BOOLEAN, NOT_FUNCTION_NAME,
            List.of(stIntersects("center", env)));
        assertThat(m.findAllSpatialMatches(not)).isEmpty();
        // A conjunct BESIDE the $or is still collected — only the disjunction's own
        // branches are excluded.
        List<SpatialConnectorMetadata.SpatialMatch> mixed =
            m.findAllSpatialMatches(and(stIntersects("geom", env), or));
        assertThat(mixed)
            .extracting(SpatialConnectorMetadata.SpatialMatch::geomName)
            .containsExactly("geom");
    }

    @Test
    void allIntersectionImplyingOpsAreCollectedForPushdown() {
        // Each of these predicates requires the geometries to intersect, so
        // overlap-only bbox/Z2 pruning is sound for all of them — a matching
        // row's bbox must overlap the query envelope.
        SpatialConnectorMetadata m = new SpatialConnectorMetadata(null, null);
        org.locationtech.jts.geom.Envelope env = new org.locationtech.jts.geom.Envelope(0, 1, 0, 1);
        for (String fn : List.of("st_intersects", "st_within", "st_contains",
                                 "st_crosses", "st_touches", "st_overlaps", "st_equals")) {
            List<SpatialConnectorMetadata.SpatialMatch> matches =
                m.findAllSpatialMatches(stCall(fn, "geom", env));
            assertThat(matches).as("%s should produce a pushdown match", fn).hasSize(1);
            assertThat(matches.get(0).functionName()).isEqualTo(fn);
            assertThat(matches.get(0).geomName()).isEqualTo("geom");
            assertThat(matches.get(0).envelopes().get(0).getMaxX()).isEqualTo(1.0);
        }
    }

    @Test
    void findAllSpatialMatchesReturnsBothGeomsForAndedPredicates() {
        SpatialConnectorMetadata m = new SpatialConnectorMetadata(null, null);
        org.locationtech.jts.geom.Envelope centerEnv  = new org.locationtech.jts.geom.Envelope(0, 1, 0, 1);
        org.locationtech.jts.geom.Envelope ellipseEnv = new org.locationtech.jts.geom.Envelope(10, 11, 10, 11);
        List<SpatialConnectorMetadata.SpatialMatch> matches = m.findAllSpatialMatches(
            and(stIntersects("center",  centerEnv),
                stIntersects("ellipse", ellipseEnv)));

        assertThat(matches).hasSize(2);
        assertThat(matches)
            .extracting(SpatialConnectorMetadata.SpatialMatch::geomName)
            .containsExactlyInAnyOrder("center", "ellipse");

        // Verify each match has the correct non-null envelope with expected bounds.
        SpatialConnectorMetadata.SpatialMatch centerMatch = matches.stream()
            .filter(s -> "center".equals(s.geomName())).findFirst().orElseThrow();
        assertThat(centerMatch.envelopes().get(0)).isNotNull();
        assertThat(centerMatch.envelopes().get(0).getMinX()).isEqualTo(0.0);
        assertThat(centerMatch.envelopes().get(0).getMaxX()).isEqualTo(1.0);
        assertThat(centerMatch.envelopes().get(0).getMinY()).isEqualTo(0.0);
        assertThat(centerMatch.envelopes().get(0).getMaxY()).isEqualTo(1.0);

        SpatialConnectorMetadata.SpatialMatch ellipseMatch = matches.stream()
            .filter(s -> "ellipse".equals(s.geomName())).findFirst().orElseThrow();
        assertThat(ellipseMatch.envelopes().get(0)).isNotNull();
        assertThat(ellipseMatch.envelopes().get(0).getMinX()).isEqualTo(10.0);
        assertThat(ellipseMatch.envelopes().get(0).getMaxX()).isEqualTo(11.0);
        assertThat(ellipseMatch.envelopes().get(0).getMinY()).isEqualTo(10.0);
        assertThat(ellipseMatch.envelopes().get(0).getMaxY()).isEqualTo(11.0);
    }

    @Test
    void extractGeomColumnNameReturnsVariableName() {
        Call call = stIntersects("center", new org.locationtech.jts.geom.Envelope(0, 1, 0, 1));
        Optional<String> name = SpatialConnectorMetadata.extractGeomColumnName(call);
        assertThat(name).contains("center");
    }

    @Test
    void spatialMatchStillFoundWithVisibilityConjunct() {
        // The datastore emits: (spatial predicate) AND is_visible(...).
        // The connector's expression walk must still find the spatial call even when
        // the visibility UDF conjunct is present in the $and node.
        SpatialConnectorMetadata m = new SpatialConnectorMetadata(null, null);
        org.locationtech.jts.geom.Envelope env = new org.locationtech.jts.geom.Envelope(0, 1, 0, 1);

        ConnectorExpression spatial = stIntersects("geom", env);

        Call visibility = new Call(BooleanType.BOOLEAN,
            new FunctionName("is_visible"),
            List.of(new Variable("visibilities", VarcharType.VARCHAR),
                    new Constant(Slices.utf8Slice("admin"), VarcharType.VARCHAR)));

        Call andExpr = new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME,
            List.of(spatial, visibility));

        List<SpatialConnectorMetadata.SpatialMatch> matches = m.findAllSpatialMatches(andExpr);
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).geomName()).isEqualTo("geom");
    }

    @Test
    void extractGeomColumnNameUnwrapsStGeomFromBinary() {
        // Production emission shape: ST_Intersects(ST_GeomFromBinary(geom), <literal>).
        // extractGeomColumnName must see through the wrap to identify the column.
        Variable geomVar = new Variable("center", VarbinaryType.VARBINARY);
        Call wrapped = new Call(
            TestGeometryType.GEOMETRY,
            new FunctionName("st_geomfrombinary"),
            List.of(geomVar));
        io.airlift.slice.Slice geomSlice = io.trino.geospatial.serde.JtsGeometrySerde.serialize(
            new GeometryFactory().toGeometry(new org.locationtech.jts.geom.Envelope(0, 1, 0, 1)));
        Call call = new Call(
            io.trino.spi.type.BooleanType.BOOLEAN,
            new FunctionName("st_intersects"),
            List.of(wrapped,
                    new Constant(geomSlice, TestGeometryType.GEOMETRY)));
        Optional<String> name = SpatialConnectorMetadata.extractGeomColumnName(call);
        assertThat(name).contains("center");
    }
}
