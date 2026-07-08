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
    void dwithinHighLatitudeOuterBoxCoversTrueRegion() throws Exception {
        // lat 60°, d = 1000 km: the poleward band edge sits at ~69.9°, where a degree of
        // longitude covers cos(69.9°)/cos(60°) ≈ 69% of what it covers at the center.
        // Sizing the outer box with cos(center) yields a ±19.8° half-span while points
        // within d near the band edge reach ~±26.1° — silently excluded. The fix sizes
        // with cos(poleward edge); assert the emitted half-span covers the true extreme.
        Filter f = ECQL.toFilter("DWITHIN(geom, POINT(10 60), 1000000, meters)");
        String sql = new TrinoFilterToSQL().encodeToString(f);
        double minX = extractBound(sql, "\"__geom_bbox__\".xmax >= ");
        double maxX = extractBound(sql, "\"__geom_bbox__\".xmin <= ");
        // True requirement (spherical): d / (111,320 m/deg × cos(69.9°)) ≈ 26.1°
        assertThat(10.0 - minX).as("west half-span").isGreaterThan(26.2);
        assertThat(maxX - 10.0).as("east half-span").isGreaterThan(26.2);
    }

    @Test
    void dwithinBandReachingPoleUsesFullLongitudeBand() throws Exception {
        // Centered at 80° with d = 1000 km the band reaches ~89.9° — effectively at the
        // pole — so the region wraps all longitudes even though the CENTER is below the
        // near-pole gate. The gate must be evaluated at the band edge.
        Filter f = ECQL.toFilter("DWITHIN(geom, POINT(10 80), 1000000, meters)");
        String sql = new TrinoFilterToSQL().encodeToString(f);
        assertThat(sql).contains("\"__geom_bbox__\".xmax >= -180");
        assertThat(sql).contains("\"__geom_bbox__\".xmin <= 180");
        assertThat(sql).doesNotContain("CASE WHEN");
    }

    @Test
    void dwithinInnerRectangleCornersNeverExceedDistance() throws Exception {
        // The THEN TRUE shortcut is sound only if every point of the inner rectangle is
        // within d of the reference. Sized with cos(center) the equatorward corners land
        // beyond d at high latitude + large radius (lat 70°, d=1000 km → ~1.03 d); the
        // fix sizes with cos(equatorward edge). Verify all four corners by haversine.
        double lat = 70.0, lon = 10.0, d = 1_000_000;
        Filter f = ECQL.toFilter("DWITHIN(geom, POINT(10 70), 1000000, meters)");
        String sql = new TrinoFilterToSQL().encodeToString(f);
        // Inner rectangle bounds live in the CASE WHEN clause (xmin >= / xmax <= form).
        String inner = sql.substring(sql.indexOf("CASE WHEN"));
        double xMin = extractBound(inner, "\"__geom_bbox__\".xmin >= ");
        double xMax = extractBound(inner, "\"__geom_bbox__\".xmax <= ");
        double yMin = extractBound(inner, "\"__geom_bbox__\".ymin >= ");
        double yMax = extractBound(inner, "\"__geom_bbox__\".ymax <= ");
        for (double cy : new double[]{yMin, yMax}) {
            for (double cx : new double[]{xMin, xMax}) {
                assertThat(haversineMeters(lat, lon, cy, cx))
                    .as("corner (%s, %s)", cx, cy)
                    .isLessThan(d);
            }
        }
    }

    /** First numeric bound following {@code marker} in the SQL. */
    private static double extractBound(String sql, String marker) {
        int i = sql.indexOf(marker);
        assertThat(i).as("marker present: " + marker).isGreaterThanOrEqualTo(0);
        int start = i + marker.length();
        int end = start;
        while (end < sql.length() && (Character.isDigit(sql.charAt(end))
                || sql.charAt(end) == '-' || sql.charAt(end) == '.'
                || sql.charAt(end) == 'E')) {
            end++;
        }
        return Double.parseDouble(sql.substring(start, end));
    }

    /** Great-circle distance on the WGS84 mean sphere. */
    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double r = 6_371_008.8;
        double p1 = Math.toRadians(lat1), p2 = Math.toRadians(lat2);
        double dp = Math.toRadians(lat2 - lat1), dl = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dp / 2) * Math.sin(dp / 2)
            + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
        return 2 * r * Math.asin(Math.sqrt(a));
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
    void intersectsNonRectangleEmitsExactStIntersectsWithoutShortcut() throws Exception {
        // L-shaped query polygon: a row whose bbox sits inside the ENVELOPE but in the
        // L's notch does NOT intersect the polygon, so the bbox-contained THEN TRUE
        // shortcut is unsound for any non-rectangular query geometry. Expect just the
        // pushable bbox-overlap prefilter AND the exact row-level ST_Intersects.
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("test");
        b.add("geom", Point.class);
        translator.setFeatureType(b.buildFeatureType());

        Filter f = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((0 0, 10 0, 10 5, 5 5, 5 10, 0 10, 0 0)))");
        String sql = translator.encodeToString(f);
        assertThat(sql).doesNotContain("CASE WHEN");
        assertThat(sql).doesNotContain("THEN TRUE");
        assertThat(sql).contains(") AND ST_Intersects(ST_GeomFromBinary(\"geom\"),");
    }

    // ── Operand order: literal-first spatial filters ──────────────────────────
    //
    // CQL permits the geometry literal on either side (e.g. INTERSECTS(POLYGON, geom)).
    // Intersects/DWithin are symmetric; Within is not — WITHIN(literal, geom) asks
    // whether the literal is contained in the row geometry.

    @Test
    void intersectsWithLiteralFirstIsHandled() throws Exception {
        Filter f = ECQL.toFilter(
            "INTERSECTS(POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)), geom)");
        String sql = translator.encodeToString(f);
        // Same predicate as the property-first form: rectangle query → bbox-overlap
        // AND CASE WHEN shortcut on the geom column's companions.
        assertThat(sql).startsWith("(\"__geom_bbox__\".xmax >= -80.0");
        assertThat(sql).contains("CASE WHEN");
        assertThat(sql).contains("ELSE ST_Intersects(ST_GeomFromBinary(\"geom\"),");
    }

    @Test
    void dwithinWithLiteralFirstIsHandled() throws Exception {
        Filter f = ECQL.toFilter("DWITHIN(POINT(-77.04 38.91), geom, 1000, meters)");
        String sql = translator.encodeToString(f);
        assertThat(sql).startsWith("(\"__geom_bbox__\".xmax >= ");
        assertThat(sql).contains("ST_Distance(to_spherical_geography(ST_GeomFromBinary(\"geom\")),");
    }

    @Test
    void withinWithLiteralFirstTestsLiteralContainedInRowGeometry() throws Exception {
        // WITHIN(literal, geom): the literal within the row geometry — the REVERSE
        // containment. Expect ST_Within(literal, geom) plus the bbox-COVERS prefilter
        // (the row bbox must contain the literal's envelope).
        Filter f = ECQL.toFilter(
            "WITHIN(POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)), geom)");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains("\"__geom_bbox__\".xmin <= -80.0");
        assertThat(sql).contains("\"__geom_bbox__\".xmax >= -70.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymin <= 37.0");
        assertThat(sql).contains("\"__geom_bbox__\".ymax >= 45.0");
        assertThat(sql).contains("AND ST_Within(ST_GeometryFromText(");
        assertThat(sql).contains("ST_GeomFromBinary(\"geom\"))");
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

    // ── Other binary spatial operators ────────────────────────────────────────

    @Test
    void crossesTouchesOverlapsEqualsGetBboxPrefilterAndExactTest() throws Exception {
        // Each of these implies a non-empty intersection, so the pushable
        // bbox-overlap prefilter is a valid necessary condition; the exact ST_*
        // runs at row level with no CASE WHEN shortcut.
        record Case(String cql, String function) {}
        var cases = java.util.List.of(
            new Case("CROSSES(geom, LINESTRING(-80 37, -70 45))", "ST_Crosses"),
            new Case("TOUCHES(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))", "ST_Touches"),
            new Case("OVERLAPS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))", "ST_Overlaps"),
            new Case("EQUALS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))", "ST_Equals"));
        for (Case c : cases) {
            String sql = new TrinoFilterToSQL().encodeToString(ECQL.toFilter(c.cql()));
            assertThat(sql).as(c.cql()).startsWith("(\"__geom_bbox__\".xmax >= -80.0");
            assertThat(sql).as(c.cql()).contains(") AND " + c.function() + "(ST_GeomFromBinary(\"geom\"),");
            assertThat(sql).as(c.cql()).doesNotContain("CASE WHEN");
        }
    }

    @Test
    void disjointHasNoBboxPrefilter() throws Exception {
        // Disjoint's result set is the complement of the overlap region — a bbox
        // prefilter would prune exactly the rows that satisfy the predicate.
        Filter f = ECQL.toFilter(
            "DISJOINT(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        String sql = translator.encodeToString(f);
        assertThat(sql).startsWith("ST_Disjoint(ST_GeomFromBinary(\"geom\"),");
        assertThat(sql).doesNotContain("__geom_bbox__");
    }

    @Test
    void beyondTranslatesToDistanceGreaterThanWithoutPrefilter() throws Exception {
        // Beyond is DWithin's complement: exact spherical distance > d, and like
        // Disjoint no bbox prefilter (the matching rows are OUTSIDE the neighborhood).
        Filter f = ECQL.toFilter("BEYOND(geom, POINT(-77.04 38.91), 100000, meters)");
        String sql = translator.encodeToString(f);
        assertThat(sql).startsWith("ST_Distance(to_spherical_geography(ST_GeomFromBinary(\"geom\")),");
        assertThat(sql).contains("> 100000");
        assertThat(sql).doesNotContain("__geom_bbox__");
    }

    @Test
    void containsPropertyFirstGetsBboxCoversPrefilterAndExactTest() throws Exception {
        // CONTAINS(geom, literal): the row geometry contains the literal, so the row
        // bbox must COVER the literal's envelope (necessary, pushable) + exact test.
        Filter f = ECQL.toFilter("CONTAINS(geom, POINT(-77.04 38.91))");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains("\"__geom_bbox__\".xmin <= -77.04");
        assertThat(sql).contains("\"__geom_bbox__\".xmax >= -77.04");
        assertThat(sql).contains("\"__geom_bbox__\".ymin <= 38.91");
        assertThat(sql).contains("\"__geom_bbox__\".ymax >= 38.91");
        assertThat(sql).contains("AND ST_Contains(ST_GeomFromBinary(\"geom\"),");
    }

    @Test
    void containsLiteralFirstIsWithinReversed() throws Exception {
        // CONTAINS(literal, geom) ⇔ WITHIN(geom, literal): for a rectangular literal
        // this collapses to the exact bbox-contained predicate — same as the Within
        // rectangle path, no row-level ST_ function at all.
        Filter f = ECQL.toFilter(
            "CONTAINS(POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)), geom)");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains(") AND (\"__geom_bbox__\".xmin >= -80.0");
        assertThat(sql).doesNotContain("ST_Contains");
        assertThat(sql).doesNotContain("ST_Within");
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
