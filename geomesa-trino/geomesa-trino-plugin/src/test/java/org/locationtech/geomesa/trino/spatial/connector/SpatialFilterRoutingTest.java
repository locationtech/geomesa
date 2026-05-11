/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.connector;

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

    /** Builds an st_disjoint call with a real Geometry constant, mirroring {@link
     *  #stIntersects}. Used to assert disjoint is NOT collected for pushdown. */
    private static Call stDisjoint(String geomCol, org.locationtech.jts.geom.Envelope env) {
        io.airlift.slice.Slice geomSlice = io.trino.geospatial.serde.JtsGeometrySerde.serialize(
            new GeometryFactory().toGeometry(env));
        Constant geomConst = new Constant(geomSlice, TestGeometryType.GEOMETRY);
        return new Call(
            io.trino.spi.type.BooleanType.BOOLEAN,
            new FunctionName("st_disjoint"),
            List.of(new Variable(geomCol, VarbinaryType.VARBINARY), geomConst));
    }

    private static Call and(ConnectorExpression... args) {
        return new Call(
            io.trino.spi.type.BooleanType.BOOLEAN,
            new FunctionName("$and"),
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
        assertThat(centerMatch.envelope()).isNotNull();
        assertThat(centerMatch.envelope().getMinX()).isEqualTo(0.0);
        assertThat(centerMatch.envelope().getMaxX()).isEqualTo(1.0);
        assertThat(centerMatch.envelope().getMinY()).isEqualTo(0.0);
        assertThat(centerMatch.envelope().getMaxY()).isEqualTo(1.0);

        SpatialConnectorMetadata.SpatialMatch ellipseMatch = matches.stream()
            .filter(s -> "ellipse".equals(s.geomName())).findFirst().orElseThrow();
        assertThat(ellipseMatch.envelope()).isNotNull();
        assertThat(ellipseMatch.envelope().getMinX()).isEqualTo(10.0);
        assertThat(ellipseMatch.envelope().getMaxX()).isEqualTo(11.0);
        assertThat(ellipseMatch.envelope().getMinY()).isEqualTo(10.0);
        assertThat(ellipseMatch.envelope().getMaxY()).isEqualTo(11.0);
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

        Call andExpr = new Call(BooleanType.BOOLEAN, new FunctionName("$and"),
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
