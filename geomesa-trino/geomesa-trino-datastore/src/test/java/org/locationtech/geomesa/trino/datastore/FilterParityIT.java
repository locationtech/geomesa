/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.filter.Filter;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.distance.DistanceOp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Filter-processing parity suite, ported from the accumulo-datastore module's
 * {@code org.locationtech.geomesa.accumulo.filter.FilterTest}: for every CQL
 * predicate, the features returned by querying the datastore (which translates
 * the filter to Trino SQL) must equal the result of evaluating the same filter
 * in memory against a known feature list.
 *
 * <p><b>Oracle design.</b> The accumulo test ingests a small synthetic feature
 * set in-test; this datastore is read-only, so the oracle is instead a bounded
 * <i>window</i> of a pre-loaded dataset: a small bbox is discovered around the
 * data's median location and every feature inside it is fetched once. Each
 * ported predicate is then ANDed with the window bbox for the datastore query,
 * and evaluated in memory over the window list — {@code datastore(filter AND
 * window) == inMemory(filter, windowFeatures)}. The window fetch itself is
 * cross-checked against a plain-SQL count through the un-wrapped iceberg
 * catalog when it is exposed.
 *
 * <p><b>Distance predicates.</b> DWITHIN/BEYOND are excluded from the plain
 * GeoTools-evaluate oracle on purpose: GeoTools evaluates the distance in
 * degrees regardless of the declared units (the accumulo suite marks these
 * {@code pendingUntilFixed} for the same reason), while this datastore emits an
 * exact spherical-meters check. The oracle here computes the great-circle
 * distance directly, and features within a small tolerance band of the radius
 * are excluded from both sides before comparing, so formula differences at the
 * boundary cannot flap the assertion.
 *
 * <p><b>Provisioning.</b> Runs against the local stack (Trino at
 * localhost:8080, catalog {@code spatial_iceberg}, schema {@code spatial});
 * each dataset's tests skip when its table is absent. The expected tables are
 * ingests of public datasets sharing the layout {@code __fid__, geom (point
 * WKB + __geom_bbox__/__geom_z2__ companions), dtg (timestamptz)} plus the
 * listed attributes:
 * <ul>
 *   <li>{@code tdrive} — Microsoft T-Drive taxi traces; {@code taxi_id} (int)</li>
 *   <li>{@code geolife} — Microsoft GeoLife trajectories; {@code user_id},
 *       {@code track_id} (string), {@code altitude_ft} (double)</li>
 *   <li>{@code ais} — MarineCadastre AIS vessel positions; {@code mmsi} (int),
 *       {@code vessel_name} (string), {@code sog} (double)</li>
 * </ul>
 */
@Tag("integration")
class FilterParityIT {

    /** Window sizing: grow/shrink the discovery bbox until the feature count
     *  lands in this range — big enough to exercise every predicate, small
     *  enough to hold in memory as the oracle. */
    private static final int MIN_WINDOW = 800;
    private static final int MAX_WINDOW = 60_000;

    /** Distance-predicate tolerance band: features whose spherical distance is
     *  within max(1.5% of the radius, 10 m) of the radius are excluded from
     *  both sides of the comparison. */
    private static final double TOLERANCE_FRACTION = 0.015;
    private static final double TOLERANCE_FLOOR_METERS = 10.0;

    private static final double EARTH_RADIUS_METERS = 6_371_008.8;

    private static DataStore ds;

    @BeforeAll
    static void connect() throws Exception {
        try (var socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress("localhost", 8080), 2000);
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Trino not reachable at localhost:8080 — skipping integration tests");
        }
        ds = DataStoreFinder.getDataStore(Map.of(
            "trino.host",    "localhost",
            "trino.port",    8080,
            "trino.catalog", "spatial_iceberg",
            "trino.schema",  "spatial"
        ));
        assertThat(ds).isNotNull();
    }

    @AfterAll
    static void disconnect() {
        if (ds != null) ds.dispose();
    }

    // ── Dataset entry points ──────────────────────────────────────────────────

    @TestFactory
    List<DynamicTest> tdrive() throws Exception {
        Fixture fx = Fixture.load("tdrive", "taxi_id", null);
        return cases(fx);
    }

    @TestFactory
    List<DynamicTest> geolife() throws Exception {
        Fixture fx = Fixture.load("geolife", "altitude_ft", "user_id");
        return cases(fx);
    }

    @TestFactory
    List<DynamicTest> ais() throws Exception {
        Fixture fx = Fixture.load("ais", "mmsi", "vessel_name");
        return cases(fx);
    }

    // ── Fixture: window oracle + derived template values ──────────────────────

    private static final class Fixture {
        final String table;
        final String geomField;
        final List<SimpleFeature> window;
        final Envelope env;
        /** Template substitutions derived from the window contents. */
        final Map<String, String> vars = new HashMap<>();
        final String stringAttr;   // null when the dataset has no usable string column

        private Fixture(String table, String geomField, List<SimpleFeature> window,
                        Envelope env, String stringAttr) {
            this.table = table;
            this.geomField = geomField;
            this.window = window;
            this.env = env;
            this.stringAttr = stringAttr;
        }

        static Fixture load(String table, String numericAttr, String stringAttr) throws Exception {
            Assumptions.assumeTrue(Arrays.asList(ds.getTypeNames()).contains(table),
                "table spatial." + table + " not ingested — skipping (see class javadoc)");
            SimpleFeatureSource src = ds.getFeatureSource(table);
            String geomField = src.getSchema().getGeometryDescriptor().getLocalName();

            // Median location of a sample, as the window center.
            Query sample = new Query(table);
            sample.setMaxFeatures(2000);
            List<Coordinate> pts = new ArrayList<>();
            try (SimpleFeatureIterator it = src.getFeatures(sample).features()) {
                while (it.hasNext()) {
                    pts.add(((Geometry) it.next().getDefaultGeometry()).getCoordinate());
                }
            }
            Assumptions.assumeTrue(pts.size() >= MIN_WINDOW,
                "table spatial." + table + " has too few rows for a parity window");
            double cx = median(pts.stream().map(c -> c.x).sorted().toList());
            double cy = median(pts.stream().map(c -> c.y).sorted().toList());

            // Grow/shrink the window until the count is workable.
            double delta = 0.005;
            Envelope env = null;
            int count = -1;
            for (int i = 0; i < 30 && (count < MIN_WINDOW || count > MAX_WINDOW); i++) {
                env = new Envelope(cx - delta, cx + delta, cy - delta, cy + delta);
                count = src.getCount(new Query(table, ECQL.toFilter(bboxCql(geomField, env))));
                delta = count < MIN_WINDOW ? delta * 2 : delta * 0.7;
            }
            Assumptions.assumeTrue(count >= MIN_WINDOW && count <= MAX_WINDOW,
                "could not find a workable parity window for spatial." + table);

            // The oracle: every feature in the window, fetched once. Its size matching
            // the pushed-down SQL count is itself a count-vs-read parity assertion.
            List<SimpleFeature> window = new ArrayList<>(count);
            try (SimpleFeatureIterator it =
                     src.getFeatures(new Query(table, ECQL.toFilter(bboxCql(geomField, env)))).features()) {
                while (it.hasNext()) {
                    window.add(it.next());
                }
            }
            assertThat(window).as("window fetch size vs pushed-down count").hasSize(count);

            Fixture fx = new Fixture(table, geomField, window, env, stringAttr);
            fx.derive(numericAttr, stringAttr);
            return fx;
        }

        /** Derives every template value from the window contents, so the suite
         *  self-configures against whatever slice of the dataset is loaded. */
        private void derive(String numericAttr, String stringAttr) {
            vars.put("G", geomField);

            // Geometry literals. PT is a real feature's coordinate (so equality and
            // small-radius distance predicates are non-trivial); R1/R2 are overlapping
            // sub-rectangles; TRI is a non-rectangle (exercises the non-rect SQL paths);
            // LINE crosses the window diagonally through PT.
            Coordinate pt = ((Geometry) window.get(0).getDefaultGeometry()).getCoordinate();
            double w = env.getWidth(), h = env.getHeight();
            double cx = env.centre().x, cy = env.centre().y;
            Envelope r1 = new Envelope(cx - 0.35 * w, cx + 0.05 * w, cy - 0.35 * h, cy + 0.05 * h);
            Envelope r2 = new Envelope(cx - 0.05 * w, cx + 0.35 * w, cy - 0.05 * h, cy + 0.35 * h);
            vars.put("PT", String.format(Locale.ROOT, "POINT (%s %s)", pt.x, pt.y));
            vars.put("R1", polyWkt(r1));
            vars.put("R2", polyWkt(r2));
            vars.put("TRI", String.format(Locale.ROOT,
                "POLYGON ((%1$s %3$s, %2$s %3$s, %1$s %4$s, %1$s %3$s))",
                r1.getMinX(), r1.getMaxX(), r1.getMinY(), r1.getMaxY()));
            vars.put("LINE", String.format(Locale.ROOT, "LINESTRING (%s %s, %s %s)",
                env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY()));
            vars.put("WINDOW", bboxCql(geomField, env));

            // Temporal boundaries: window dtg quantiles, truncated to whole seconds so
            // the SQL literal and the in-memory Date are bit-identical.
            List<Date> dtgs = window.stream()
                .map(f -> (Date) f.getAttribute("dtg"))
                .filter(Objects::nonNull)
                .sorted()
                .toList();
            assertThat(dtgs).as("dtg attribute must be populated").isNotEmpty();
            vars.put("T1", isoSecond(dtgs.get(dtgs.size() / 4)));
            vars.put("T2", isoSecond(dtgs.get(dtgs.size() / 2)));
            vars.put("T3", isoSecond(dtgs.get(3 * dtgs.size() / 4)));

            // Numeric attribute: the value of a real feature, plus quartile bounds.
            vars.put("NATTR", numericAttr);
            List<Double> nums = window.stream()
                .map(f -> (Number) f.getAttribute(numericAttr))
                .filter(Objects::nonNull)
                .map(Number::doubleValue)
                .sorted()
                .toList();
            assertThat(nums).as(numericAttr + " must be populated").isNotEmpty();
            vars.put("NVAL", trimNum(nums.get(nums.size() / 2)));
            vars.put("NLO",  trimNum(nums.get(nums.size() / 4)));
            vars.put("NHI",  trimNum(nums.get(3 * nums.size() / 4)));

            // String attribute (when the dataset has one): the most common simple
            // value in the window, and a LIKE prefix derived from it.
            if (stringAttr != null) {
                Map<String, Long> freq = window.stream()
                    .map(f -> (String) f.getAttribute(stringAttr))
                    .filter(v -> v != null && !v.isEmpty() && v.matches("[A-Za-z0-9 _.-]+"))
                    .collect(Collectors.groupingBy(v -> v, Collectors.counting()));
                Assumptions.assumeTrue(!freq.isEmpty(),
                    stringAttr + " has no plain values in the window — skipping");
                String sval = freq.entrySet().stream()
                    .max(Map.Entry.comparingByValue()).orElseThrow().getKey();
                vars.put("SATTR", stringAttr);
                vars.put("SVAL", sval);
                vars.put("SPRE", sval.substring(0, Math.min(3, sval.length())));
            }

            // Feature IDs of real window features (start, middle, end).
            vars.put("FID1", window.get(0).getID());
            vars.put("FID2", window.get(window.size() / 2).getID());
            vars.put("FID3", window.get(window.size() - 1).getID());
        }

        String fill(String template) {
            String out = template;
            for (Map.Entry<String, String> e : vars.entrySet()) {
                out = out.replace("${" + e.getKey() + "}", e.getValue());
            }
            assertThat(out).as("unresolved placeholder in: " + out).doesNotContain("${");
            return out;
        }
    }

    // ── Ported filter cases ───────────────────────────────────────────────────

    private List<DynamicTest> cases(Fixture fx) {
        List<DynamicTest> tests = new ArrayList<>();
        tests.add(DynamicTest.dynamicTest("window count matches plain-catalog SQL",
            () -> windowCountMatchesPlainSql(fx)));

        for (String cql : standardCases(fx)) {
            tests.add(DynamicTest.dynamicTest(cql, () -> runStandard(fx, cql)));
        }
        for (DistanceCase dc : distanceCases(fx)) {
            tests.add(DynamicTest.dynamicTest(dc.cql, () -> runDistance(fx, dc)));
        }
        return tests;
    }

    /** The ported predicate set, adapted from the accumulo-datastore TestFilters
     *  lists: same categories and operator combinations, with the synthetic
     *  attr/val/geometry constants replaced by values derived from the window. */
    private static List<String> standardCases(Fixture fx) {
        List<String> templates = new ArrayList<>();

        // goodSpatialPredicates (+ the operators this datastore supports beyond them)
        templates.addAll(List.of(
            "INTERSECTS(${G}, ${R1})",
            "INTERSECTS(${G}, ${TRI})",            // non-rectangle: no CASE WHEN shortcut
            "OVERLAPS(${G}, ${R1})",
            "WITHIN(${G}, ${R1})",
            "WITHIN(${G}, ${TRI})",
            "CONTAINS(${G}, ${R1})",
            "CONTAINS(${G}, ${PT})",
            "CROSSES(${G}, ${R1})",
            "TOUCHES(${G}, ${R1})",
            "EQUALS(${G}, ${PT})",
            "BBOX(${G}, ${BB1})",
            "DISJOINT(${G}, ${R1})",
            // literal-first operand order (the swapped path in visitBinarySpatialOperator)
            "INTERSECTS(${R1}, ${G})",
            "WITHIN(${R1}, ${G})",
            "CONTAINS(${R1}, ${G})"
        ));

        // andedSpatialPredicates / oredSpatialPredicates: the full op-pair matrices
        List<String> ops = List.of("INTERSECTS", "OVERLAPS", "WITHIN", "DISJOINT", "CROSSES");
        for (String a : ops) {
            for (String b : ops) {
                if (!a.equals(b)) {
                    templates.add(a + "(${G}, ${R1}) AND " + b + "(${G}, ${R2})");
                    templates.add(a + "(${G}, ${R1}) OR "  + b + "(${G}, ${R2})");
                }
            }
        }

        // simpleNotFilters
        templates.addAll(List.of(
            "NOT (INTERSECTS(${G}, ${R1}))",
            "NOT (WITHIN(${G}, ${TRI}))",
            "NOT (BBOX(${G}, ${BB1}))",
            "NOT (${NATTR} = ${NVAL})",
            "NOT (dtg DURING ${T1}/${T3})",
            "NOT (dtg AFTER ${T2})"
        ));

        // temporalPredicates
        templates.addAll(List.of(
            "dtg DURING ${T1}/${T2}",
            "dtg DURING ${T2}/${T3}",
            "dtg BEFORE ${T2}",
            "dtg AFTER ${T2}",
            "dtg BETWEEN '${T1}' AND '${T2}'",
            "(not dtg after ${T2}) and (not dtg before ${T1})"
        ));

        // spatioTemporalPredicates
        templates.addAll(List.of(
            "INTERSECTS(${G}, ${R1}) AND dtg DURING ${T1}/${T2}",
            "WITHIN(${G}, ${R2}) AND dtg AFTER ${T2}"
        ));

        // attributePredicates (numeric; every dataset has one)
        templates.addAll(List.of(
            "${NATTR} = ${NVAL}",
            "${NATTR} <> ${NVAL}",
            "${NATTR} < ${NHI}",
            "${NATTR} > ${NLO}",
            "${NATTR} <= ${NHI}",
            "${NATTR} >= ${NLO}",
            "${NATTR} BETWEEN ${NLO} AND ${NHI}",
            "${NATTR} IN (${NLO}, ${NVAL}, ${NHI})"
        ));

        // attributePredicates (string, where the dataset has a string column)
        if (fx.stringAttr != null) {
            templates.addAll(List.of(
                "${SATTR} = '${SVAL}'",
                "${SATTR} LIKE '${SPRE}%'",
                "${SATTR} ILIKE '${SPRE}%'",
                "${SATTR} ILIKE '%${SPRE}%'",
                "${SATTR} ILIKE 'zzzzzz%'"     // known miss, mirroring "ILIKE '1%'"
            ));
        }

        // attributeAndGeometricPredicates
        templates.addAll(List.of(
            "${NATTR} = ${NVAL} AND INTERSECTS(${G}, ${R1})",
            "${NATTR} > ${NLO} AND WITHIN(${G}, ${TRI})",
            "${NATTR} <= ${NHI} AND BBOX(${G}, ${BB1})",
            "INTERSECTS(${G}, ${R1}) AND ${NATTR} BETWEEN ${NLO} AND ${NHI}"
        ));
        if (fx.stringAttr != null) {
            templates.addAll(List.of(
                "${SATTR} = '${SVAL}' AND INTERSECTS(${G}, ${R1})",
                "${SATTR} ILIKE '${SPRE}%' AND WITHIN(${G}, ${R2})"
            ));
        }

        // andsOrsFilters / oneGeomFilters: nested and/or combinations
        templates.addAll(List.of(
            "((INTERSECTS(${G}, ${R1}) OR INTERSECTS(${G}, ${R2}))"
                + " AND (dtg DURING ${T1}/${T3} OR ${NATTR} = ${NVAL}))",
            "((dtg DURING ${T1}/${T3} AND INTERSECTS(${G}, ${R1}))"
                + " OR (${NATTR} = ${NVAL} OR dtg DURING ${T2}/${T3}))",
            "((${NATTR} = ${NVAL} AND dtg DURING ${T1}/${T2})"
                + " AND (INTERSECTS(${G}, ${R2}) OR ${NATTR} > ${NHI} OR INTERSECTS(${G}, ${R1})))",
            "(dtg DURING ${T1}/${T2} OR ${NATTR} = ${NVAL} OR dtg DURING ${T2}/${T3})",
            "(INTERSECTS(${G}, ${R1}) OR dtg DURING ${T2}/${T3})",
            "(INTERSECTS(${G}, ${R1}) AND dtg DURING ${T1}/${T3} AND dtg DURING ${T1}/${T2})"
        ));

        // idPredicates
        templates.addAll(List.of(
            "IN('${FID1}','${FID2}')",
            "IN('${FID3}')",
            "IN('${FID2}','${FID3}') AND IN('${FID1}')",     // contradiction: empty
            "IN('${FID1}','${FID2}') AND dtg DURING ${T1}/${T3}",
            "dtg DURING ${T1}/${T3} AND IN('${FID1}')",
            "WITHIN(${G}, ${R1}) AND IN('${FID1}')",
            "IN('${FID1}') AND BBOX(${G}, ${BB1})",
            "IN('${FID1}','${FID2}') AND ${NATTR} >= ${NLO} AND WITHIN(${G}, ${WINPOLY})"
        ));

        // Resolve the helper placeholders used above.
        Envelope r1 = envOf(fx.vars.get("R1"));
        fx.vars.put("BB1", String.format(Locale.ROOT, "%s, %s, %s, %s",
            r1.getMinX(), r1.getMinY(), r1.getMaxX(), r1.getMaxY()));
        fx.vars.put("WINPOLY", polyWkt(fx.env));

        return templates.stream().map(fx::fill).toList();
    }

    /** DWITHIN/BEYOND, kept structural so the oracle can compute spherical
     *  distances to the same reference geometry. Radii scale with the window;
     *  the extended-geometry references exercise the envelope-expansion path. */
    private record DistanceCase(String cql, String refWkt, double meters, boolean beyond) {}

    private static List<DistanceCase> distanceCases(Fixture fx) {
        String pt = fx.vars.get("PT");
        String line = fx.vars.get("LINE");
        String poly = fx.vars.get("R1");
        double windowMeters = haversine(fx.env.getMinY(), fx.env.getMinX(),
                                        fx.env.getMinY(), fx.env.getMaxX());
        double small = Math.max(50, windowMeters / 10);
        double medium = windowMeters / 2;
        double large = windowMeters * 2;
        List<DistanceCase> cases = new ArrayList<>();
        for (double r : new double[]{1.0, small, medium, large}) {
            cases.add(new DistanceCase(dwithinCql(fx, pt, r), pt, r, false));
        }
        cases.add(new DistanceCase(dwithinCql(fx, line, small), line, small, false));
        cases.add(new DistanceCase(dwithinCql(fx, poly, small), poly, small, false));
        cases.add(new DistanceCase(
            String.format(Locale.ROOT, "BEYOND(%s, %s, %.1f, meters)", fx.geomField, pt, medium),
            pt, medium, true));
        cases.add(new DistanceCase(
            String.format(Locale.ROOT, "BEYOND(%s, %s, %.1f, meters)", fx.geomField, line, small),
            line, small, true));
        return cases;
    }

    private static String dwithinCql(Fixture fx, String refWkt, double meters) {
        return String.format(Locale.ROOT, "DWITHIN(%s, %s, %.1f, meters)",
            fx.geomField, refWkt, meters);
    }

    // ── Execution ─────────────────────────────────────────────────────────────

    private static void runStandard(Fixture fx, String cql) throws Exception {
        Filter filter = ECQL.toFilter(cql);
        Set<String> oracle = fx.window.stream()
            .filter(filter::evaluate)
            .map(SimpleFeature::getID)
            .collect(Collectors.toSet());
        Set<String> actual = queryFids(fx, cql);
        assertParity(cql, actual, oracle, fx.window.size());
    }

    private static void runDistance(Fixture fx, DistanceCase dc) throws Exception {
        Geometry ref = new WKTReader().read(dc.refWkt());
        double tolerance = Math.max(dc.meters() * TOLERANCE_FRACTION, TOLERANCE_FLOOR_METERS);

        Set<String> oracle = new HashSet<>();
        Set<String> boundary = new HashSet<>();
        for (SimpleFeature f : fx.window) {
            double d = sphericalDistance((Geometry) f.getDefaultGeometry(), ref);
            if (Math.abs(d - dc.meters()) <= tolerance) {
                boundary.add(f.getID());
            } else if (dc.beyond() ? d > dc.meters() : d <= dc.meters()) {
                oracle.add(f.getID());
            }
        }
        // A fid can appear on both sides when duplicate fids exist (multiple rows per
        // fid — real in GeoLife) with one row in range and another in the tolerance
        // band; drop band-touching fids from BOTH sides so they can't flap the result.
        oracle.removeAll(boundary);
        Set<String> actual = queryFids(fx, dc.cql());
        actual.removeAll(boundary);
        assertParity(dc.cql(), actual, oracle, fx.window.size());
    }

    /** Runs {@code cql AND <window bbox>} through the datastore, returning FIDs. */
    private static Set<String> queryFids(Fixture fx, String cql) throws Exception {
        String bounded = "(" + cql + ") AND " + fx.vars.get("WINDOW");
        Set<String> fids = new HashSet<>();
        try (SimpleFeatureIterator it = ds.getFeatureSource(fx.table)
                 .getFeatures(new Query(fx.table, ECQL.toFilter(bounded))).features()) {
            while (it.hasNext()) {
                fids.add(it.next().getID());
            }
        }
        return fids;
    }

    private static void assertParity(String cql, Set<String> actual, Set<String> oracle, int windowSize) {
        Set<String> missing = new HashSet<>(oracle);
        missing.removeAll(actual);
        Set<String> extra = new HashSet<>(actual);
        extra.removeAll(oracle);
        assertThat(actual)
            .as("datastore results vs in-memory oracle for [%s] (window=%d, oracle=%d, actual=%d;"
                    + " missing=%s extra=%s)",
                cql, windowSize, oracle.size(), actual.size(),
                sample(missing), sample(extra))
            .isEqualTo(oracle);
    }

    private static String sample(Set<String> fids) {
        return fids.stream().limit(5).collect(Collectors.joining(",", "[", fids.size() > 5 ? ",…]" : "]"));
    }

    /** Cross-checks the window feature count against a plain SQL count through the
     *  un-wrapped iceberg catalog, independently of everything this plugin adds.
     *  Skips when the plain catalog is not exposed (hardened deployments). */
    private static void windowCountMatchesPlainSql(Fixture fx) throws Exception {
        // Bounds MUST be DOUBLE literals: Trino parses a bare 17-digit decimal as
        // DECIMAL, and the DECIMAL→DOUBLE coercion can land one ulp away from the
        // Java double the window was computed with (observed: 39.067550000000004
        // as DECIMAL coerces one ulp BELOW the Java double, admitting a boundary
        // row the window correctly excludes).
        String sql = String.format(Locale.ROOT,
            "SELECT count(*) FROM iceberg.spatial.%s WHERE"
                + " ST_X(ST_GeomFromBinary(%s)) BETWEEN DOUBLE '%s' AND DOUBLE '%s'"
                + " AND ST_Y(ST_GeomFromBinary(%s)) BETWEEN DOUBLE '%s' AND DOUBLE '%s'",
            fx.table,
            fx.geomField, fx.env.getMinX(), fx.env.getMaxX(),
            fx.geomField, fx.env.getMinY(), fx.env.getMaxY());
        long plain;
        try (var conn = java.sql.DriverManager.getConnection(
                 "jdbc:trino://localhost:8080/iceberg?user=geomesa");
             var st = conn.createStatement();
             var rs = st.executeQuery(sql)) {
            rs.next();
            plain = rs.getLong(1);
        } catch (java.sql.SQLException e) {
            Assumptions.assumeTrue(false, "plain iceberg catalog not exposed — skipping cross-check");
            return;
        }
        assertThat(fx.window.size())
            .as("window size vs plain-catalog SQL count (env=%s, table=%s)", fx.env, fx.table)
            .isEqualTo((int) plain);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static double median(List<Double> sorted) {
        return sorted.get(sorted.size() / 2);
    }

    private static String bboxCql(String geomField, Envelope e) {
        return String.format(Locale.ROOT, "BBOX(%s, %s, %s, %s, %s)",
            geomField, e.getMinX(), e.getMinY(), e.getMaxX(), e.getMaxY());
    }

    private static String polyWkt(Envelope e) {
        return String.format(Locale.ROOT,
            "POLYGON ((%1$s %3$s, %2$s %3$s, %2$s %4$s, %1$s %4$s, %1$s %3$s))",
            e.getMinX(), e.getMaxX(), e.getMinY(), e.getMaxY());
    }

    /** Envelope of a rectangle WKT emitted by {@link #polyWkt}. */
    private static Envelope envOf(String rectWkt) {
        try {
            return new WKTReader().read(rectWkt).getEnvelopeInternal();
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new IllegalArgumentException(rectWkt, e);
        }
    }

    /** Whole-second ISO instant, so CQL parsing and SQL literals agree exactly. */
    private static String isoSecond(Date d) {
        return Instant.ofEpochSecond(d.getTime() / 1000).toString();
    }

    /** Renders a numeric template value, dropping a trailing ".0" for integers. */
    private static String trimNum(double v) {
        return v == Math.rint(v) && !Double.isInfinite(v)
            ? String.valueOf((long) v) : String.valueOf(v);
    }

    /** Great-circle distance (m) between a feature geometry and a reference geometry:
     *  nearest pair in coordinate space, then haversine between them. Exact for point
     *  references; a close approximation for lines/polygons (the tolerance band in
     *  runDistance absorbs the difference). */
    private static double sphericalDistance(Geometry a, Geometry b) {
        Coordinate[] nearest = DistanceOp.nearestPoints(a, b);
        return haversine(nearest[0].y, nearest[0].x, nearest[1].y, nearest[1].x);
    }

    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double p1 = Math.toRadians(lat1), p2 = Math.toRadians(lat2);
        double dp = Math.toRadians(lat2 - lat1), dl = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dp / 2) * Math.sin(dp / 2)
            + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
        return 2 * EARTH_RADIUS_METERS * Math.asin(Math.sqrt(a));
    }
}
