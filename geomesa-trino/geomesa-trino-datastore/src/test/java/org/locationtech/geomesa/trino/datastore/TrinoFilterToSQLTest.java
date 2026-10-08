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
import org.geotools.api.filter.PropertyIsEqualTo;
import org.geotools.api.filter.identity.FeatureId;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.filter.text.cql2.CQL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.geomesa.filter.factory.FastFilterFactory;
import org.locationtech.geomesa.filter.function.CurrentDateFunction;
import org.locationtech.geomesa.utils.json.JsonPathPropertyNameResolver;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The translator must emit PURE row-level ST_* predicates: no bbox-struct
 * comparisons, no CASE WHEN shortcuts. All bbox/Z2/XZ2 pushdown is derived
 * connector-side ({@code SpatialConnectorMetadata.applyFilter}) from exactly the
 * {@code ST_*(ST_GeomFromBinary(col), ST_GeometryFromText('…'))} shape asserted
 * here, so datastore queries and hand-written Trino SQL get identical plans.
 * Result-set semantics are covered end-to-end by the filter-parity suite
 * ({@code FilterParityIT}).
 */
class TrinoFilterToSQLTest {

    private TrinoFilterToSQL translator;
    private FilterFactory ff;

    @BeforeEach
    void setUp() {
        translator = new TrinoFilterToSQL();
        ff = CommonFactoryFinder.getFilterFactory();
    }

    /** The composite-SQL shapes this translator must never emit again. */
    private static void assertPure(String sql) {
        assertThat(sql).doesNotContain("_bbox__");
        assertThat(sql).doesNotContain("CASE WHEN");
        assertThat(sql).doesNotContain("THEN TRUE");
    }

    // ── Intersects / BBOX ─────────────────────────────────────────────────────

    @Test
    void bboxTranslatesToPureStIntersectsOfEnvelopeRectangle() throws Exception {
        // BBOX(geom, env) ⇔ INTERSECTS(geom, envelope-rectangle). The exact row-level
        // test also keeps the predicate correct against float32-rounded stored bboxes
        // (a bare bbox-overlap admits rows up to ½ ulp outside the envelope).
        Filter f = ff.bbox("geom", -80.0, 37.0, -70.0, 45.0, "EPSG:4326");
        String sql = translator.encodeToString(f);
        assertPure(sql);
        assertThat(sql).startsWith("ST_Intersects(ST_GeomFromBinary(\"geom\"), ST_GeometryFromText('POLYGON");
        Envelope env = firstGeometryLiteral(sql).getEnvelopeInternal();
        assertThat(env).isEqualTo(new Envelope(-80.0, -70.0, 37.0, 45.0));
    }

    @Test
    void intersectsEmissionIsShapeAndSchemaIndependent() throws Exception {
        // Historically rectangles, non-rectangles, and point-schema columns produced
        // three different composite shapes. Pure emission is identical for all: the
        // connector, not this translator, decides what to derive from the predicate.
        String rect = "POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))";
        String lShape = "POLYGON ((0 0, 10 0, 10 5, 5 5, 5 10, 0 10, 0 0))";
        for (String poly : new String[]{rect, lShape}) {
            String sql = new TrinoFilterToSQL().encodeToString(
                ECQL.toFilter("INTERSECTS(geom, " + poly + ")"));
            assertPure(sql);
            assertThat(sql).isEqualTo(
                "ST_Intersects(ST_GeomFromBinary(\"geom\"), ST_GeometryFromText('" + poly + "'))");
        }
        // Declaring a Point-typed schema must not change the emission either.
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("test");
        b.add("geom", Point.class);
        translator.setFeatureType(b.buildFeatureType());
        String sql = translator.encodeToString(ECQL.toFilter("INTERSECTS(geom, " + rect + ")"));
        assertThat(sql).isEqualTo(
            "ST_Intersects(ST_GeomFromBinary(\"geom\"), ST_GeometryFromText('" + rect + "'))");
    }

    @Test
    void intersectsWithLiteralFirstIsHandled() throws Exception {
        // Symmetric operator: literal-first form emits the identical column-first call.
        String sql = translator.encodeToString(ECQL.toFilter(
            "INTERSECTS(POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)), geom)"));
        assertPure(sql);
        assertThat(sql).startsWith("ST_Intersects(ST_GeomFromBinary(\"geom\"),");
    }

    // ── Within / Contains (asymmetric: swapped forms normalize column-first) ──

    @Test
    void withinTranslatesToPureStWithin() throws Exception {
        String sql = translator.encodeToString(ECQL.toFilter(
            "WITHIN(geom, POLYGON((-80 37, -70 37, -75 45, -80 37)))"));  // triangle
        assertPure(sql);
        assertThat(sql).isEqualTo("ST_Within(ST_GeomFromBinary(\"geom\"),"
            + " ST_GeometryFromText('POLYGON ((-80 37, -70 37, -75 45, -80 37))'))");
    }

    @Test
    void withinLiteralFirstNormalizesToColumnFirstStContains() throws Exception {
        // WITHIN(literal, geom) ⇔ CONTAINS(geom, literal) (OGC complement). Emitting
        // the column-first form matters: the connector's recognizer expects
        // ST_*(ST_GeomFromBinary(col), literal) and would skip pushdown otherwise.
        String sql = translator.encodeToString(ECQL.toFilter(
            "WITHIN(POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)), geom)"));
        assertPure(sql);
        assertThat(sql).startsWith("ST_Contains(ST_GeomFromBinary(\"geom\"),");
    }

    @Test
    void containsPropertyFirstTranslatesToPureStContains() throws Exception {
        String sql = translator.encodeToString(
            ECQL.toFilter("CONTAINS(geom, POINT(-77.04 38.91))"));
        assertPure(sql);
        assertThat(sql).isEqualTo("ST_Contains(ST_GeomFromBinary(\"geom\"),"
            + " ST_GeometryFromText('POINT (-77.04 38.91)'))");
    }

    @Test
    void containsLiteralFirstNormalizesToColumnFirstStWithin() throws Exception {
        // CONTAINS(literal, geom) ⇔ WITHIN(geom, literal).
        String sql = translator.encodeToString(ECQL.toFilter(
            "CONTAINS(POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)), geom)"));
        assertPure(sql);
        assertThat(sql).startsWith("ST_Within(ST_GeomFromBinary(\"geom\"),");
    }

    // ── Other binary spatial operators ────────────────────────────────────────

    @Test
    void crossesTouchesOverlapsEqualsTranslateToPureStCalls() throws Exception {
        record Case(String cql, String function) {}
        var cases = java.util.List.of(
            new Case("CROSSES(geom, LINESTRING(-80 37, -70 45))", "ST_Crosses"),
            new Case("TOUCHES(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))", "ST_Touches"),
            new Case("OVERLAPS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))", "ST_Overlaps"),
            new Case("EQUALS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))", "ST_Equals"));
        for (Case c : cases) {
            String sql = new TrinoFilterToSQL().encodeToString(ECQL.toFilter(c.cql()));
            assertPure(sql);
            assertThat(sql).as(c.cql())
                .startsWith(c.function() + "(ST_GeomFromBinary(\"geom\"), ST_GeometryFromText(");
        }
    }

    @Test
    void disjointTranslatesToPureStDisjoint() throws Exception {
        // The connector deliberately derives NO pushdown from st_disjoint (overlap-based
        // pruning would drop the answer set) — the pure call is all there is.
        String sql = translator.encodeToString(ECQL.toFilter(
            "DISJOINT(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))"));
        assertPure(sql);
        assertThat(sql).startsWith("ST_Disjoint(ST_GeomFromBinary(\"geom\"),");
    }

    // ── DWithin / Beyond ──────────────────────────────────────────────────────
    //
    // DWithin emits: ST_Intersects(geom, <outer-envelope rectangle>) AND <exact
    // spherical distance ≤ d>. The ST_Intersects conjunct is a NECESSARY condition
    // (every geometry within d of the reference overlaps the outer envelope) in the
    // exact shape the connector derives bbox/Z2 pruning from; the outer-envelope
    // geometry math below (poleward-edge scaling, near-pole bands, envelope
    // expansion for extended references) is asserted by parsing the emitted WKT.

    @Test
    void dwithinEmitsOuterEnvelopeIntersectsAndExactSphericalDistance() throws Exception {
        String sql = translator.encodeToString(
            ECQL.toFilter("DWITHIN(geom, POINT(-77.04 38.91), 100000, meters)"));
        assertPure(sql);
        assertThat(sql).startsWith("ST_Intersects(ST_GeomFromBinary(\"geom\"), ST_GeometryFromText('POLYGON");
        assertThat(sql).contains(") AND ST_Distance(to_spherical_geography(ST_GeomFromBinary(\"geom\")),"
            + " to_spherical_geography(ST_GeometryFromText(");
        assertThat(sql).contains("<= 100000");
        // The outer envelope covers the point ± ~1° lat (100 km × 1.1 margin).
        Envelope outer = firstGeometryLiteral(sql).getEnvelopeInternal();
        assertThat(outer.getMinY()).isLessThan(38.91 - 0.9);
        assertThat(outer.getMaxY()).isGreaterThan(38.91 + 0.9);
        assertThat(outer.getMinX()).isLessThan(-77.04 - 0.9);
        assertThat(outer.getMaxX()).isGreaterThan(-77.04 + 0.9);
    }

    @Test
    void dwithinNearPoleUsesFullLongitudeBand() throws Exception {
        // lat 87 ≥ NEAR_POLE_LAT (85): the flat cos(lat) longitude scaling degenerates
        // (→ division-by-zero at the pole, and a within-d region that wraps all
        // longitudes). The outer envelope must span every longitude so the pruning
        // conjunct never drops a matching row.
        String sql = translator.encodeToString(
            ECQL.toFilter("DWITHIN(geom, POINT(10 87), 100000, meters)"));
        assertPure(sql);
        Envelope outer = firstGeometryLiteral(sql).getEnvelopeInternal();
        assertThat(outer.getMinX()).isEqualTo(-180.0);
        assertThat(outer.getMaxX()).isEqualTo(180.0);
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
        String sql = new TrinoFilterToSQL().encodeToString(
            ECQL.toFilter("DWITHIN(geom, POINT(10 60), 1000000, meters)"));
        Envelope outer = firstGeometryLiteral(sql).getEnvelopeInternal();
        assertThat(10.0 - outer.getMinX()).as("west half-span").isGreaterThan(26.2);
        assertThat(outer.getMaxX() - 10.0).as("east half-span").isGreaterThan(26.2);
    }

    @Test
    void dwithinBandReachingPoleUsesFullLongitudeBand() throws Exception {
        // Centered at 80° with d = 1000 km the band reaches ~89.9° — effectively at the
        // pole — so the region wraps all longitudes even though the CENTER is below the
        // near-pole gate. The gate must be evaluated at the band edge.
        String sql = new TrinoFilterToSQL().encodeToString(
            ECQL.toFilter("DWITHIN(geom, POINT(10 80), 1000000, meters)"));
        Envelope outer = firstGeometryLiteral(sql).getEnvelopeInternal();
        assertThat(outer.getMinX()).isEqualTo(-180.0);
        assertThat(outer.getMaxX()).isEqualTo(180.0);
    }

    @Test
    void dwithinLinestringOuterBoxCoversFarEnds() throws Exception {
        // For an extended reference the within-d region surrounds the WHOLE geometry. A
        // radius-d box around the envelope centroid excludes rows near the far ends:
        // for this ~470 km linestring and d = 1 km, a point 550 m from the (48, 27)
        // endpoint sits ~230 km from the centroid — silently pruned. The outer box must
        // be the reference envelope EXPANDED by d.
        String sql = translator.encodeToString(
            ECQL.toFilter("DWITHIN(geom, LINESTRING(45 23, 48 27), 1000, meters)"));
        assertPure(sql);
        Envelope outer = firstGeometryLiteral(sql).getEnvelopeInternal();
        assertThat(outer.getMinX()).as("west bound covers west endpoint + d").isLessThan(45.0);
        assertThat(outer.getMaxX()).as("east bound covers east endpoint + d").isGreaterThan(48.0049);
        assertThat(outer.getMinY()).as("south bound covers south endpoint + d").isLessThan(23.0);
        assertThat(outer.getMaxY()).as("north bound covers north endpoint + d").isGreaterThan(27.0049);
        // Trino's spherical ST_Distance only accepts points, so the extended reference
        // goes through the planar nearest-points pair first.
        assertThat(sql).contains(") AND ST_Distance(to_spherical_geography("
            + "geometry_nearest_points(ST_GeomFromBinary(\"geom\"), ST_GeometryFromText(");
        assertThat(sql).contains("LINESTRING (45 23, 48 27)");
    }

    @Test
    void dwithinPolygonReferenceUsesEnvelopeExpandedOuterBox() throws Exception {
        String sql = translator.encodeToString(ECQL.toFilter(
            "DWITHIN(geom, POLYGON((45 23, 48 23, 48 27, 45 27, 45 23)), 1000, meters)"));
        assertPure(sql);
        Envelope outer = firstGeometryLiteral(sql).getEnvelopeInternal();
        assertThat(outer.getMinX()).isLessThan(45.0);
        assertThat(outer.getMaxX()).isGreaterThan(48.0);
        assertThat(sql).contains("<= 1000");
    }

    @Test
    void dwithinWithLiteralFirstIsHandled() throws Exception {
        String sql = translator.encodeToString(
            ECQL.toFilter("DWITHIN(POINT(-77.04 38.91), geom, 1000, meters)"));
        assertPure(sql);
        assertThat(sql).startsWith("ST_Intersects(ST_GeomFromBinary(\"geom\"),");
        assertThat(sql).contains("ST_Distance(to_spherical_geography(ST_GeomFromBinary(\"geom\")),");
    }

    @Test
    void beyondTranslatesToDistanceGreaterThanWithoutPrefilter() throws Exception {
        // Beyond is DWithin's complement: exact spherical distance > d, and like
        // Disjoint no prefilter (the matching rows are OUTSIDE the neighborhood).
        String sql = translator.encodeToString(
            ECQL.toFilter("BEYOND(geom, POINT(-77.04 38.91), 100000, meters)"));
        assertPure(sql);
        assertThat(sql).startsWith("ST_Distance(to_spherical_geography(ST_GeomFromBinary(\"geom\")),");
        assertThat(sql).contains("> 100000");
    }

    // ── Temporal ──────────────────────────────────────────────────────────────

    @Test
    void currentDateCutoffsAreEvaluatedAsTimestampLiterals() throws Exception {
        for (String offset : List.of("", "-P1D", "P2D")) {
            String expression = offset.isEmpty() ? "currentDate()" : "currentDate('" + offset + "')";
            Duration duration = offset.isEmpty() ? Duration.ZERO : Duration.parse(offset);
            for (String comparison : List.of("<", ">=")) {
                Instant before = Instant.now().plus(duration).truncatedTo(ChronoUnit.MILLIS);
                String sql = translator.encodeToString(ECQL.toFilter("dtg " + comparison + " " + expression));
                Instant after = Instant.now().plus(duration).truncatedTo(ChronoUnit.MILLIS);
                assertThat(sql).startsWith("\"dtg\" " + comparison + " TIMESTAMP '");
                assertThat(sql).doesNotContain("currentDate");
                String timestamp = sql.substring(sql.indexOf("TIMESTAMP '") + 11, sql.length() - 5);
                Instant cutoff = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]"))
                    .toInstant(ZoneOffset.UTC);
                assertThat(cutoff).isBetween(before, after);
            }
        }
    }

    @Test
    void mergedTimeRangeRetainsQueryAndEvaluatesStoreCutoff() throws Exception {
        Filter query = ECQL.toFilter("dtg DURING 2026-10-05T10:16:00Z/2026-10-05T11:16:00Z");
        Filter storeFilter = ECQL.toFilter("dtg < currentDate('-P1D')");
        String sql = translator.encodeToString(ff.and(query, storeFilter));
        assertThat(sql).contains("\"dtg\" > TIMESTAMP '2026-10-05 10:16:00 UTC'");
        assertThat(sql).contains("\"dtg\" < TIMESTAMP '2026-10-05 11:16:00 UTC'");
        assertThat(sql).doesNotContain("currentDate");
        assertThat(sql.split("TIMESTAMP '")).hasSize(4);
    }

    @Test
    void currentDateIsReevaluatedForReusedFilters() throws Exception {
        AtomicInteger evaluations = new AtomicInteger();
        var function = new CurrentDateFunction() {
            @Override
            public Object evaluate(Object feature) {
                return Date.from(Instant.parse("2026-10-07T00:00:00Z")
                    .plus(evaluations.getAndIncrement(), ChronoUnit.DAYS));
            }
        };
        Filter filter = ff.less(ff.property("dtg"), function);
        assertThat(translator.encodeToString(filter)).isEqualTo("\"dtg\" < TIMESTAMP '2026-10-07 00:00:00 UTC'");
        assertThat(translator.encodeToString(filter)).isEqualTo("\"dtg\" < TIMESTAMP '2026-10-08 00:00:00 UTC'");
    }

    @Test
    void featureDependentFunctionsRetainTheirPropertyArguments() throws Exception {
        Filter filter = ff.equal(ff.function("strToLowerCase", ff.property("name")), ff.literal("alice"), true);
        assertThat(translator.encodeToString(filter)).contains("strToLowerCase(\"name\")").contains("'alice'");
    }

    @Test
    void implicitFeatureFunctionsUseFeatureIdAndDefaultGeometry() throws Exception {
        var builder = new SimpleFeatureTypeBuilder();
        builder.setName("test");
        builder.add("geom", Point.class);
        translator.setFeatureType(builder.buildFeatureType());
        assertThat(translator.encodeToString(ff.equal(ff.function("proxyId"), ff.literal(1), true)))
            .contains("geomesa_proxy_id(\"__fid__\", false)");
        for (String name : List.of("z2", "xz2")) {
            assertThat(translator.encodeToString(ff.equal(ff.function(name), ff.literal("value"), true)))
                .contains("geomesa_" + name + "(\"geom\")").doesNotContain("NULL");
        }
        translator.getFeatureType().getUserData().put("geomesa.fid.uuid", true);
        assertThat(translator.encodeToString(ff.equal(ff.function("proxyId"), ff.literal(1), true)))
            .contains("geomesa_proxy_id(\"__fid__\", true)");
    }

    @Test
    void geomesaFunctionTranslationsUseNativeSqlAndRegisteredUdfs() throws Exception {
        var builder = new SimpleFeatureTypeBuilder();
        builder.setName("test");
        builder.add("name", String.class);
        builder.add("dtg", Date.class);
        builder.add("geom", Point.class);
        translator.setFeatureType(builder.buildFeatureType());
        assertThat(translator.encodeToString(ECQL.toFilter("dateToLong(dtg) > 0")))
            .contains("date_diff('millisecond', TIMESTAMP '1970-01-01 00:00:00 UTC', \"dtg\")");
        assertThat(translator.encodeToString(ff.equal(ff.function("fastproperty", ff.literal(0L)), ff.literal("a"), true)))
            .contains("\"name\" = 'a'").doesNotContain("fastproperty");
        assertThat(translator.encodeToString(ECQL.toFilter("murmurHash(name) > 0")))
            .contains("geomesa_murmur_hash(CAST(\"name\" AS varchar))");
        assertThat(translator.encodeToString(ECQL.toFilter("bucketHash(name, 8) = 1")))
            .contains("mod(bitwise_and(geomesa_murmur_hash(CAST(\"name\" AS varchar)), 2147483647), 8)");
        assertThat(translator.encodeToString(ECQL.toFilter("z2(geom) = 'a'")))
            .contains("geomesa_z2(\"geom\")");
        assertThat(translator.encodeToString(ECQL.toFilter("xz2(geom) = 'a'")))
            .contains("geomesa_xz2(\"geom\")");
        assertThat(translator.encodeToString(ECQL.toFilter("convert2viewer(name, geom, dtg) = 'a'")))
            .contains("geomesa_convert2viewer(\"name\", \"geom\", date_diff('millisecond',");
        assertThat(translator.encodeToString(ECQL.toFilter("convert2viewer(name, geom, 1234) = 'a'")))
            .contains("geomesa_convert2viewer(\"name\", \"geom\", 1234)");
    }

    @Test
    void hashTranslationPreservesGeoMesaValueTypes() throws Exception {
        Object[] values = {"name", 1, 1L, 1.0f, 1.0d, new Date(0), new byte[]{0, -1},
            UUID.fromString("fedcba98-7654-3210-ffff-0123456789ab")};
        String[] types = {"varchar", "bigint", "bigint", "real", "double", "timestamp(3) with time zone", "varbinary", "uuid"};
        for (int i = 0; i < values.length; i++) {
            Filter filter = ff.equal(ff.function("murmurHash", ff.literal(values[i])), ff.literal(1), true);
            assertThat(translator.encodeToString(filter)).contains(" AS " + types[i] + "))");
        }
        assertThatThrownBy(() -> translator.encodeToString(
            ff.equal(ff.function("murmurHash", ff.literal(true)), ff.literal(1), true)))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("value type");
    }

    @Test
    void dateFunctionsRetainMillisecondsBeforeAndAfterEpoch() throws Exception {
        for (long millis : new long[]{1234, -1}) {
            var date = ff.literal(new Date(millis));
            for (String name : List.of("dateToLong", "murmurHash")) {
                String sql = translator.encodeToString(ff.equal(ff.function(name, date), ff.literal(1), true));
                assertThat(sql).contains(millis == 1234
                    ? "TIMESTAMP '1970-01-01 00:00:01.234 UTC'"
                    : "TIMESTAMP '1969-12-31 23:59:59.999 UTC'");
            }
        }
    }

    @Test
    void temporalComparisonLiteralsAreTimestampTyped() throws Exception {
        // BEFORE/AFTER (and BETWEEN etc.) route Date literals through writeLiteral —
        // they must emit TIMESTAMP literals, not quoted strings, or Trino rejects the
        // varchar-vs-timestamptz comparison (caught by the filter-parity suite).
        String after = translator.encodeToString(
            ECQL.toFilter("dtg AFTER 2023-06-01T12:30:00Z"));
        assertThat(after).contains("\"dtg\" > TIMESTAMP '2023-06-01 12:30:00");
        assertThat(after).doesNotContain("> '2023");
        String before = translator.encodeToString(
            ECQL.toFilter("dtg BEFORE 2023-06-01T12:30:00Z"));
        assertThat(before).contains("\"dtg\" < TIMESTAMP '2023-06-01 12:30:00");
    }

    @Test
    void duringTranslatesToTimestampRange() throws Exception {
        Filter f = ECQL.toFilter(
            "dtg DURING 2023-01-01T00:00:00Z/2024-01-01T00:00:00Z");
        String sql = translator.encodeToString(f);
        assertThat(sql).contains("\"dtg\" > TIMESTAMP '2023-01-01 00:00:00 UTC'");
        assertThat(sql).contains("\"dtg\" < TIMESTAMP '2024-01-01 00:00:00 UTC'");
    }

    // ── Composition / identifiers / feature ids ───────────────────────────────

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

    // ── JSON path (structural types) ──────────────────────────────────────────
    //
    // A property name that starts with `$` is a JSON path into a structural (json=true +
    // json-schema) attribute; it translates to a Trino ROW dereference "props"."name". The
    // base class still writes the operator and literal. Non-pushable paths (opaque variant,
    // wildcards, indices, functions, unknown attribute) throw. Schemaless VARIANT string
    // equality can separately supply a necessary prefilter while retaining its residual.

    /** A schema with a structural `props` attribute and an opaque `blob` (json=true, no
     *  json-schema), plus a geometry for the AND-composition case. */
    private void setStructuralSchema() {
        SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
        b.setName("test");
        b.add("geom", Point.class);
        b.userData("json", "true").userData("json-schema", "record");
        b.add("props", String.class);
        b.userData("json", "true");  // opaque variant: no json-schema
        b.add("blob", String.class);
        translator.setFeatureType(b.buildFeatureType());
    }

    @Test
    void jsonPathTranslatesToRowDereference() throws Exception {
        setStructuralSchema();
        String sql = translator.encodeToString(ECQL.toFilter("\"$.props.name\" = 'alice'"));
        assertThat(sql).isEqualTo("\"props\".\"name\" = 'alice'");
    }

    @Test
    void jsonPathDeepDereference() throws Exception {
        setStructuralSchema();
        String sql = translator.encodeToString(ECQL.toFilter("\"$.props.nested.city\" = 'ny'"));
        assertThat(sql).isEqualTo("\"props\".\"nested\".\"city\" = 'ny'");
    }

    @Test
    void jsonPathNumericLeafKeepsLiteralUnquoted() throws Exception {
        setStructuralSchema();
        String sql = translator.encodeToString(ECQL.toFilter("\"$.props.age\" > 30"));
        assertThat(sql).isEqualTo("\"props\".\"age\" > 30");
    }

    @Test
    void jsonPathComposesWithSpatialPredicate() throws Exception {
        setStructuralSchema();
        String sql = translator.encodeToString(ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))"
            + " AND \"$.props.name\" = 'alice'"));
        assertThat(sql).contains("ST_Intersects(ST_GeomFromBinary(\"geom\"),");
        assertThat(sql).contains("\"props\".\"name\" = 'alice'");
        assertThat(sql).contains(" AND ");
    }

    @Test
    void jsonPathOnOpaqueVariantThrows() {
        setStructuralSchema();
        assertThatThrownBy(() -> translator.encodeToString(ECQL.toFilter("\"$.blob.name\" = 'alice'")))
            .hasMessageContaining("opaque");
    }

    @Test
    void variantEqualityMatchingCaseSurvivesParsingAndPropertyResolution() throws Exception {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("variant_case");
        builder.userData("json", "true");
        builder.add("blob", String.class);
        var schema = builder.buildFeatureType();
        translator.setFeatureType(schema);
        var property = ff.property("$.blob.name");
        var literal = ff.literal("alice");
        for (Filter filter : List.of(
                CQL.toFilter("\"$.blob.name\" = 'alice'"),
                ECQL.toFilter("\"$.blob.name\" = 'alice'"),
                ECQL.toFilter("'alice' = \"$.blob.name\""),
                ff.equals(property, literal), ff.equal(property, literal, true))) {
            assertThat(((PropertyIsEqualTo) filter).isMatchingCase()).isTrue();
            var resolved = (PropertyIsEqualTo) filter.accept(new JsonPathPropertyNameResolver(schema), null);
            assertThat(resolved.isMatchingCase()).isTrue();
            assertThat(translator.variantPrefilter(resolved)).isNotNull();
        }
        var insensitive = ff.equal(property, literal, false);
        var resolved = (PropertyIsEqualTo) insensitive.accept(new JsonPathPropertyNameResolver(schema), null);
        assertThat(resolved.isMatchingCase()).isFalse();
        assertThat(resolved.evaluate(SimpleFeatureBuilder.build(
            schema, new Object[]{"{\"name\":\"ALICE\"}"}, "case"))).isTrue();
        assertThat(translator.variantPrefilter(resolved)).isNull();
    }

    @Test
    void optimizedJsonEqualityWithFalseMatchingCaseFallsBackToResidual() throws Exception {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("variant_case");
        builder.userData("json", "true");
        builder.add("blob", String.class);
        var schema = builder.buildFeatureType();
        translator.setFeatureType(schema);
        // GeoMesa's fast JSON equality reports false even for ordinary case-sensitive ECQL.
        // Keep the conservative guard: a false flag must never enable a case-sensitive prefilter.
        var optimized = (PropertyIsEqualTo) FastFilterFactory.toFilter(schema, "\"$.blob.name\" = 'alice'");
        assertThat(optimized.isMatchingCase()).isFalse();
        assertThat(optimized.evaluate(SimpleFeatureBuilder.build(
            schema, new Object[]{"{\"name\":\"alice\"}"}, "match"))).isTrue();
        assertThat(optimized.evaluate(SimpleFeatureBuilder.build(
            schema, new Object[]{"{\"name\":\"ALICE\"}"}, "case"))).isFalse();
        assertThat(translator.variantPrefilter(optimized)).isNull();
    }

    @Test
    void variantStringEqualityPrefilterKeepsOtherTypesForResidual() throws Exception {
        setStructuralSchema();
        String sql = translator.variantPrefilter(ECQL.toFilter("\"$.blob.name\" = 'alice'"));
        assertThat(sql).contains("TRY(\"blob\"['name'])");
        assertThat(sql).contains("AS VARCHAR) AS VARIANT)");
        assertThat(sql).contains("THEN TRY(\"blob\"['name']) = CAST('alice' AS VARIANT) ELSE TRUE END");
        assertThat(translator.variantPrefilter(ECQL.toFilter("'alice' = \"$.blob.name\""))).isEqualTo(sql);
    }

    @Test
    void variantPrefilterEscapesNestedKeysAndLiterals() throws Exception {
        setStructuralSchema();
        assertThat(translator.variantPrefilter(ff.equals(ff.property("$.blob.nested.name"), ff.literal("O'Brien"))))
            .contains("\"blob\"['nested']['name']", "CAST('O''Brien' AS VARIANT)");
        assertThat(translator.variantPrefilter(ff.equals(ff.property("$.blob['odd\\'name']"), ff.literal("x"))))
            .contains("['odd''name']");
    }

    @Test
    void variantPrefilterRejectsNegationCaseFoldingAndUnsupportedOperators() throws Exception {
        setStructuralSchema();
        for (String cql : new String[]{"NOT (\"$.blob.name\" = 'alice')", "\"$.blob.name\" <> 'alice'",
                "\"$.blob.age\" > 30", "\"$.blob.age\" = 30", "\"$.props.name\" = 'alice'",
                "\"$.blob\" = 'alice'", "\"$.blob.name\" IS NULL"}) {
            assertThat(translator.variantPrefilter(ECQL.toFilter(cql))).as(cql).isNull();
        }
        assertThat(translator.variantPrefilter(ff.equal(ff.property("$.blob.name"), ff.literal("alice"), false)))
            .isNull();
        for (String path : new String[]{"$.blob.*", "$.blob.tags[0]", "$.blob.scores.max()", "$.missing.name"}) {
            assertThatThrownBy(() -> translator.variantPrefilter(ff.equals(ff.property(path), ff.literal("x"))))
                .isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void jsonPathWildcardThrows() {
        setStructuralSchema();
        assertThatThrownBy(() -> translator.encodeToString(ECQL.toFilter("\"$.props.*\" = 'alice'")))
            .isInstanceOf(RuntimeException.class);
    }

    @Test
    void jsonPathArrayIndexThrows() {
        setStructuralSchema();
        assertThatThrownBy(() -> translator.encodeToString(ECQL.toFilter("\"$.props.tags[0]\" = 'x'")))
            .isInstanceOf(RuntimeException.class);
    }

    @Test
    void jsonPathFunctionThrows() {
        setStructuralSchema();
        assertThatThrownBy(() -> translator.encodeToString(ECQL.toFilter("\"$.props.scores.max()\" > 5")))
            .hasMessageContaining("path functions");
    }

    @Test
    void jsonPathUnknownAttributeThrows() {
        setStructuralSchema();
        assertThatThrownBy(() -> translator.encodeToString(ECQL.toFilter("\"$.missing.name\" = 'x'")))
            .hasMessageContaining("does not point at an attribute");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** The first {@code ST_GeometryFromText('…')} literal in the SQL, parsed. For the
     *  DWithin emission this is the outer-envelope rectangle (the reference geometry
     *  appears later, inside the distance expression). */
    private static org.locationtech.jts.geom.Geometry firstGeometryLiteral(String sql) throws Exception {
        String marker = "ST_GeometryFromText('";
        int start = sql.indexOf(marker);
        assertThat(start).as("geometry literal present").isGreaterThanOrEqualTo(0);
        start += marker.length();
        int end = sql.indexOf("')", start);
        assertThat(end).as("geometry literal terminated").isGreaterThan(start);
        return new WKTReader().read(sql.substring(start, end));
    }
}
