/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TrinoSchemaDiscoveryTest {

    @Test
    void discoversCompanionStyleVisColumn() {
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("__fid__", "geom", "__vis__"))).isEqualTo("__vis__");
    }

    @Test
    void bareVisibilitiesColumnIsNotTreatedAsVisibility() {
        // A user attribute merely named "visibilities" is ordinary data, not an
        // enforcement column — only the companion-style __vis__ engages filtering.
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("__fid__", "geom", "visibilities"))).isNull();
    }

    @Test
    void absentVisColumnYieldsNull() {
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("__fid__", "geom", "dtg"))).isNull();
    }

    @Test
    void z2CompanionMarksPointColumn() {
        assertThat(TrinoSchemaDiscovery.isPointColumn("center",
            Set.of("center", "__center_z2__", "__center_bbox__"))).isTrue();
    }

    @Test
    void xz2CompanionIsNotPointColumn() {
        assertThat(TrinoSchemaDiscovery.isPointColumn("ellipse",
            Set.of("ellipse", "__ellipse_xz2__", "__ellipse_bbox__"))).isFalse();
    }

    @Test
    void sftBindingWinsOverHeuristic() {
        // The stored SFT declares the exact subtype; it overrides the companion heuristic —
        // both directions: an XZ2 column (heuristic would say generic Geometry) bound to
        // Polygon, and a Z2 column (heuristic would say Point) bound to whatever the SFT says.
        var allNames = Set.of("region", "__region_xz2__", "__region_bbox__");
        assertThat(TrinoSchemaDiscovery.resolveGeometryBinding(
            "region", allNames, Map.of("region", Polygon.class))).isEqualTo(Polygon.class);
        assertThat(TrinoSchemaDiscovery.resolveGeometryBinding(
            "center", Set.of("center", "__center_z2__"), Map.of("center", Polygon.class)))
            .isEqualTo(Polygon.class);
    }

    @Test
    void parsesSftWithOptionsAndUserData() {
        String spec = "id:String:fs.bounds=true,dtg:Date:default=true:fs.bounds=true,"
            + "name:String,count:Long,score:Double:fs.bounds=true,active:Boolean,"
            + "*geom:Point:srid=4326;geomesa.index.dtg='dtg'";
        Map<String, Class<?>> bindings =
            TrinoSchemaDiscovery.geometryBindingsFromSpec("example", spec);
        assertThat(bindings).containsExactly(Map.entry("geom", Point.class));
    }

    @Test
    void multiGeomSpecYieldsEachSubtype() {
        Map<String, Class<?>> bindings = TrinoSchemaDiscovery.geometryBindingsFromSpec(
            "t", "*center:Point:srid=4326,ellipse:Polygon:srid=4326,dtg:Date,label:String");
        assertThat(bindings).containsOnly(
            Map.entry("center", Point.class), Map.entry("ellipse", Polygon.class));
    }

    @Test
    void unparseableSpecYieldsEmptyForHeuristicFallback() {
        assertThat(TrinoSchemaDiscovery.geometryBindingsFromSpec("t", "]not a valid spec["))
            .isEmpty();
    }

    @Test
    void fallsBackToHeuristicWhenSftAbsent() {
        // No SFT entry for this column → z2 companion ⇒ Point, xz2 companion ⇒ generic Geometry.
        assertThat(TrinoSchemaDiscovery.resolveGeometryBinding(
            "center", Set.of("center", "__center_z2__"), Map.of())).isEqualTo(Point.class);
        assertThat(TrinoSchemaDiscovery.resolveGeometryBinding(
            "ellipse", Set.of("ellipse", "__ellipse_xz2__"), Map.of())).isEqualTo(Geometry.class);
    }
}
