/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.identity.FeatureId;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TrinoFilterToSQLTest {

    private TrinoFilterToSQL translator;
    private FilterFactory ff;

    @BeforeEach
    void setUp() {
        translator = new TrinoFilterToSQL();
        ff = CommonFactoryFinder.getFilterFactory();
    }

    @Test
    void bboxTranslatesToBboxStructColumns() throws Exception {
        Filter f = ff.bbox("geom", -80.0, 37.0, -70.0, 45.0, "EPSG:4326");
        String sql = translator.encodeToString(f);
        assertThat(sql).isEqualTo(
            "\"__geom_bbox__\".xmax >= -80.0 AND \"__geom_bbox__\".xmin <= -70.0" +
            " AND \"__geom_bbox__\".ymax >= 37.0 AND \"__geom_bbox__\".ymin <= 45.0");
    }

    @Test
    void intersectsTranslatesToBboxOverlapAndCaseWhenContainedShortcut() throws Exception {
        Filter f = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        String sql = translator.encodeToString(f);
        // Two-part predicate:
        //   1. bbox-overlap (pushable) — necessary for ST_Intersects=TRUE; SI's
        //      tryExtractBboxEnvelope reads this shape and pushes Z2 file-level pruning.
        //   2. CASE WHEN bbox-contained — sufficient for ST_Intersects=TRUE; survives
        //      Trino's optimizer and short-circuits the WKB decode + intersect test.
        assertThat(sql).startsWith("(\"__geom_bbox__\".xmax >= -80.0");
        assertThat(sql).contains("\"__geom_bbox__\".xmin <= -70.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymax >= 37.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymin <= 45.0");
        assertThat(sql).contains(") AND CASE WHEN \"__geom_bbox__\".xmin >= -80.0");
        assertThat(sql).contains("\"__geom_bbox__\".xmax <= -70.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymin >= 37.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymax <= 45.0");
        assertThat(sql).contains("THEN TRUE");
        assertThat(sql).contains("ELSE ST_Intersects(ST_GeomFromBinary(\"geom\"),");
        assertThat(sql).contains("POLYGON");
        assertThat(sql).endsWith("END");
    }

    @Test
    void withinRectangleTranslatesToBboxOverlapAndBboxContainedNoRowLevelStWithin() throws Exception {
        // Rectangular query polygon: bbox⊆rect ⇔ ST_Within(geom, rect). Exact
        // equivalence — no row-level ST_Within needed. Emit bbox-overlap (for SI's
        // Z2 pushdown via the existing pattern) AND bbox-contained (the exact predicate).
        Filter f = ECQL.toFilter(
            "WITHIN(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        String sql = translator.encodeToString(f);
        assertThat(sql).startsWith("(\"__geom_bbox__\".xmax >= -80.0");
        assertThat(sql).contains("\"__geom_bbox__\".xmin <= -70.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymax >= 37.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymin <= 45.0");
        assertThat(sql).contains(") AND (\"__geom_bbox__\".xmin >= -80.0");
        assertThat(sql).contains("\"__geom_bbox__\".xmax <= -70.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymin >= 37.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymax <= 45.0");
        assertThat(sql).doesNotContain("ST_Within");
        assertThat(sql).doesNotContain("CASE");
    }

    @Test
    void withinNonRectangleTranslatesToBboxOverlapAndStWithin() throws Exception {
        // Non-rectangular polygon: must keep ST_Within at row level (bbox⊆env(polygon)
        // doesn't prove geom⊆polygon — the polygon is a strict subset of its envelope).
        // Still emit bbox-overlap as a leading conjunct so SI can push Z2 partitioning.
        Filter f = ECQL.toFilter(
            "WITHIN(geom, POLYGON((-80 37, -70 37, -75 45, -80 37)))");  // triangle
        String sql = translator.encodeToString(f);
        assertThat(sql).startsWith("(\"__geom_bbox__\".xmax >= -80.0");
        assertThat(sql).contains(") AND ST_Within(ST_GeomFromBinary(\"geom\"),");
        assertThat(sql).contains("POLYGON");
    }

    @Test
    void dwithinTranslatesToOuterBboxOverlapAndCaseWhenInnerInscribedShortcut() throws Exception {
        Filter f = ECQL.toFilter("DWITHIN(geom, POINT(-77.04 38.91), 100000, meters)");
        String sql = translator.encodeToString(f);
        // Outer bbox-overlap prefilter (necessary): pushable to file-level pruning.
        assertThat(sql).startsWith("(\"__geom_bbox__\".xmax >= ");
        assertThat(sql).contains("\"__geom_bbox__\".xmin <= ");
        assertThat(sql).contains("\"__geom_bbox__\".ymax >= ");
        assertThat(sql).contains("\"__geom_bbox__\".ymin <= ");
        // CASE WHEN inner-rectangle-contained shortcut: sufficient for distance ≤ d.
        assertThat(sql).contains(") AND CASE WHEN \"__geom_bbox__\".xmin >= ");
        assertThat(sql).contains("\"__geom_bbox__\".xmax <= ");
        assertThat(sql).contains("\"__geom_bbox__\".ymin >= ");
        assertThat(sql).contains("\"__geom_bbox__\".ymax <= ");
        assertThat(sql).contains(" THEN TRUE");
        // Exact spherical distance is the ELSE branch — ST_Distance on spherical_geography
        // measured in meters, with geom converted via ST_GeomFromBinary.
        assertThat(sql).contains("ELSE ST_Distance(to_spherical_geography(ST_GeomFromBinary(\"geom\")),");
        assertThat(sql).contains("to_spherical_geography(ST_GeometryFromText(");
        assertThat(sql).contains("<= 100000");
        assertThat(sql).endsWith(" END");
    }

    @Test
    void dwithinNearPoleUsesFullLongitudeBandAndSkipsInscribedShortcut() throws Exception {
        // lat 87 ≥ NEAR_POLE_LAT (85): the flat cos(lat) longitude scaling degenerates
        // (→ division-by-zero at the pole, and a within-d region that wraps all longitudes).
        // The outer prefilter must span every longitude so no matching row is dropped, and the
        // inscribed-rectangle TRUE shortcut is skipped in favor of the exact distance check.
        Filter f = ECQL.toFilter("DWITHIN(geom, POINT(10 87), 100000, meters)");
        String sql = translator.encodeToString(f);
        // Full-longitude outer band: xmax >= -180 ... xmin <= 180 (no bounded, row-dropping span).
        assertThat(sql).contains("\"__geom_bbox__\".xmax >= -180");
        assertThat(sql).contains("\"__geom_bbox__\".xmin <= 180");
        // No inscribed-rectangle shortcut near the pole — no false-positive TRUE branch.
        assertThat(sql).doesNotContain("CASE WHEN");
        assertThat(sql).doesNotContain("THEN TRUE");
        // Exact spherical distance is ANDed directly as the sole row-level check.
        assertThat(sql).contains(") AND ST_Distance(to_spherical_geography(ST_GeomFromBinary(\"geom\")),");
        assertThat(sql).contains("<= 100000");
    }

    @Test
    void duringTranslatesToTimestampRange() throws Exception {
        Filter f = ECQL.toFilter(
            "dtg DURING 2023-01-01T00:00:00Z/2024-01-01T00:00:00Z");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains("\"dtg\" > TIMESTAMP '2023-01-01 00:00:00 UTC'");
        assertThat(sql).contains("\"dtg\" < TIMESTAMP '2024-01-01 00:00:00 UTC'");
    }

    @Test
    void columnIdentifiersAreQuotedInEmittedSql() throws Exception {
        Filter spatial = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -75 45, -80 37)))");  // triangle
        assertThat(translator.encodeToString(spatial))
            .contains("ST_GeomFromBinary(\"geom\")");

        Filter dwithin = ECQL.toFilter("DWITHIN(geom, POINT(-77.04 38.91), 1000, meters)");
        assertThat(new TrinoFilterToSQL().encodeToString(dwithin))
            .contains("ST_GeomFromBinary(\"geom\")");

        Filter within = ECQL.toFilter(
            "WITHIN(geom, POLYGON((-80 37, -70 37, -75 45, -80 37)))");  // triangle
        assertThat(new TrinoFilterToSQL().encodeToString(within))
            .contains("ST_Within(ST_GeomFromBinary(\"geom\"),");
    }

    @Test
    void intersectsAndDuringJoinedWithAnd() throws Exception {
        Filter f = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))" +
            " AND dtg DURING 2023-01-01T00:00:00Z/2024-01-01T00:00:00Z");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains("ST_Intersects(ST_GeomFromBinary(\"geom\"),");
        assertThat(sql).contains("\"dtg\" > TIMESTAMP");
        assertThat(sql).contains(" AND ");
    }

    @Test
    void fidInTranslatesToFidColumnIn() throws Exception {
        Set<FeatureId> ids = Set.of(
            ff.featureId("abc-123"),
            ff.featureId("def-456"));
        Filter f = ff.id(ids);
        String sql = translator.encodeToString(f);
        assertThat(sql).startsWith("\"__fid__\" IN (");
        assertThat(sql).contains("'abc-123'");
        assertThat(sql).contains("'def-456'");
    }

    // ── ST_Intersects rectangle + point fast path ─────────────────────────────
    //
    // For axis-aligned rectangle R and Point data:
    //   bbox(point) = point, so bbox-overlap(point, R) ⇔ point ∈ R ⇔ ST_Intersects.
    // The CASE WHEN bbox-contained fallback in the general shortcut is dead code
    // here — it always returns TRUE on the same rows bbox-overlap passes. Emit
    // bbox-overlap alone and skip the per-row CASE evaluation.
    //
    // Both conditions are required: rectangle (so bbox-overlap is sufficient,
    // not just necessary) AND point geometry (so bbox-overlap is necessary AND
    // sufficient, not just necessary).

    @Test
    void intersectsRectangleOnPointDataTranslatesToBboxOverlapOnly() throws Exception {
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("test");
        b.add("geom", Point.class);
        translator.setFeatureType(b.buildFeatureType());

        Filter f = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        String sql = translator.encodeToString(f);

        // Same predicate shape as a plain BBOX filter — no CASE, no ST_Intersects,
        // no WKB-decode in the per-row work.
        assertThat(sql).isEqualTo(
            "\"__geom_bbox__\".xmax >= -80.0 AND \"__geom_bbox__\".xmin <= -70.0" +
            " AND \"__geom_bbox__\".ymax >= 37.0 AND \"__geom_bbox__\".ymin <= 45.0");
    }

    @Test
    void intersectsRectangleOnPolygonDataFallsBackToCaseWhenShortcut() throws Exception {
        // Non-point geometry column: bbox(g) is a strict superset of g (e.g., a
        // diagonal line's bbox can overlap a query rectangle the line misses).
        // bbox-overlap-only is unsound; keep the CASE WHEN shortcut with row-level
        // ST_Intersects fallback.
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("test");
        b.add("geom", Polygon.class);
        translator.setFeatureType(b.buildFeatureType());

        Filter f = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains("CASE WHEN");
        assertThat(sql).contains("ELSE ST_Intersects(ST_GeomFromBinary(\"geom\"),");
    }

    @Test
    void intersectsNonRectangleOnPointDataKeepsCaseWhenShortcut() throws Exception {
        // L-shaped query polygon: a point inside the envelope but inside the L's
        // notch is NOT inside the polygon. bbox-overlap-only would over-include.
        // The CASE WHEN bbox-contained predicate still catches the rows fully
        // inside the envelope; ELSE catches the notch.
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("test");
        b.add("geom", Point.class);
        translator.setFeatureType(b.buildFeatureType());

        Filter f = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((0 0, 10 0, 10 5, 5 5, 5 10, 0 10, 0 0)))");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains("CASE WHEN");
        assertThat(sql).contains("ELSE ST_Intersects(ST_GeomFromBinary(\"geom\"),");
    }

    @Test
    void intersectsRectangleWithoutFeatureTypeFallsBackToCaseWhenShortcut() throws Exception {
        // GeoTools allows FilterToSQL without a FeatureType. If we can't prove
        // the column is Point, we must keep the safe general shortcut form.
        Filter f = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains("CASE WHEN");
        assertThat(sql).contains("ELSE ST_Intersects(ST_GeomFromBinary(\"geom\"),");
    }

    @Test
    void attributeAndIntersectsJoinedWithAnd() throws Exception {
        Filter f = ECQL.toFilter(
            "active = TRUE AND value > 50.0 AND " +
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains("active");
        assertThat(sql).contains("value");
        assertThat(sql).contains("ST_Intersects(ST_GeomFromBinary(\"geom\"),");
    }
}
