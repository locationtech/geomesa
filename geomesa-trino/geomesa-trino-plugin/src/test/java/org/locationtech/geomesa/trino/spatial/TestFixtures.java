/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Lazy provisioning of the demo tables the integration tests need. When a
 * table is absent, {@link #ensureTable} creates it (via the plain {@code
 * iceberg} catalog — the spatial connector intentionally blocks writes) and
 * loads the bundled fixture from {@code src/test/resources/fixtures/}, so the
 * ITs run against any Trino with the plugin installed, no external ingest
 * required. When ingested demo tables already exist (e.g. the full synthetic
 * datasets), they are used as-is.
 *
 * <p>The fixtures were exported from live ingested tables (see
 * {@code export_it_fixtures.py} in the companion tooling), so companion
 * values — bbox floats, hex-encoded Z2/XZ2 cells — match real writer output
 * bit-for-bit. Visibility is stored as a tier index (0 = unrestricted,
 * 1 = partial, 2 = full) and rendered here from the same
 * {@code -Dgeomesa.it.auths.partial} / {@code -Dgeomesa.it.auths.full}
 * properties the visibility ITs read, so fixture data always matches the
 * tokens the tests query with.
 *
 * <p>Provisioning requires the plain {@code iceberg} catalog to be exposed
 * and writable; where it isn't (hardened deployments), {@link #ensureTable}
 * returns {@code false} and callers skip via JUnit assumptions as before.
 */
public final class TestFixtures {

    private static final String SPATIAL_URL = "jdbc:trino://localhost:8080/spatial_iceberg?user=admin";
    private static final String PLAIN_URL   = "jdbc:trino://localhost:8080/iceberg?user=admin";

    private static final String PARTIAL_AUTH =
        System.getProperty("geomesa.it.auths.partial", "basic");
    private static final String FULL_AUTHS =
        System.getProperty("geomesa.it.auths.full", "basic,privileged");

    private static final int BATCH_ROWS = 200;

    private TestFixtures() {}

    /**
     * Ensures the named demo table exists, provisioning it from the bundled
     * fixture when absent. Returns false when the table neither exists nor
     * could be provisioned (no bundled fixture, plain catalog not exposed,
     * or not writable) — callers should skip.
     */
    public static boolean ensureTable(String table) {
        try {
            if (tableExists(table)) {
                return true;
            }
            switch (table) {
                case "observations" -> provisionObservations();
                case "regions" -> provisionRegions();
                case "observations_2geom" -> provisionObservations2Geom();
                default -> {
                    return false;  // no bundled fixture (e.g. tdrive/geolife/ais)
                }
            }
            return tableExists(table);
        } catch (Exception e) {
            System.err.println("TestFixtures: could not provision spatial." + table
                + " — " + e.getMessage());
            return false;
        }
    }

    private static boolean tableExists(String table) throws SQLException {
        // information_schema, not SHOW TABLES: the latter THROWS when the spatial
        // schema itself doesn't exist yet (fresh warehouse), which must read as
        // "absent, provision it" rather than an error.
        try (Connection c = DriverManager.getConnection(SPATIAL_URL);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1 FROM information_schema.tables"
                 + " WHERE table_schema = 'spatial' AND table_name = '" + table + "'")) {
            return rs.next();
        }
    }

    private static void provisionObservations() throws Exception {
        try (Connection c = DriverManager.getConnection(PLAIN_URL);
             Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS spatial");
            s.execute("CREATE TABLE spatial.observations ("
                + "\"__fid__\" varchar NOT NULL, geom varbinary,"
                + " dtg timestamp(6) with time zone,"
                + " sensor_id varchar, value double, active boolean, \"__vis__\" varchar,"
                + " \"__geom_bbox__\" ROW(xmin real, ymin real, xmax real, ymax real),"
                + " \"__geom_z2__\" varchar)"
                + " WITH (partitioning = ARRAY['truncate(__geom_z2__, 2)','month(dtg)'])");
            List<String> tuples = new ArrayList<>();
            for (String[] r : readFixture("/fixtures/observations.tsv")) {
                // __fid__, geom_wkb_hex, dtg, sensor_id, value, active, vis_tier,
                // bbox xmin/ymin/xmax/ymax, z2_hex
                tuples.add("(" + str(r[0]) + ", from_hex(" + str(r[1]) + "),"
                    + " TIMESTAMP '" + r[2] + "', " + str(r[3]) + ", " + r[4] + ", " + r[5] + ","
                    + " " + visExpr(r[6]) + ","
                    + " " + bbox(r[7], r[8], r[9], r[10]) + ", " + str(r[11]) + ")");
                flushBatch(s, "spatial.observations", tuples, false);
            }
            flushBatch(s, "spatial.observations", tuples, true);
        }
    }

    private static void provisionRegions() throws Exception {
        try (Connection c = DriverManager.getConnection(PLAIN_URL);
             Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS spatial");
            s.execute("CREATE TABLE spatial.regions ("
                + "\"__fid__\" varchar NOT NULL, geom varbinary,"
                + " dtg timestamp(6) with time zone, category varchar,"
                + " \"__geom_bbox__\" ROW(xmin real, ymin real, xmax real, ymax real),"
                + " \"__geom_xz2__\" varchar)"
                + " WITH (partitioning = ARRAY['truncate(__geom_xz2__, 4)','month(dtg)'])");
            List<String> tuples = new ArrayList<>();
            for (String[] r : readFixture("/fixtures/regions.tsv")) {
                // __fid__, geom_wkb_hex, dtg, category, bbox xmin/ymin/xmax/ymax, xz2_hex
                tuples.add("(" + str(r[0]) + ", from_hex(" + str(r[1]) + "),"
                    + " TIMESTAMP '" + r[2] + "', " + str(r[3]) + ","
                    + " " + bbox(r[4], r[5], r[6], r[7]) + ", " + str(r[8]) + ")");
                flushBatch(s, "spatial.regions", tuples, false);
            }
            flushBatch(s, "spatial.regions", tuples, true);
        }
    }

    private static void provisionObservations2Geom() throws Exception {
        try (Connection c = DriverManager.getConnection(PLAIN_URL);
             Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS spatial");
            s.execute("CREATE TABLE spatial.observations_2geom ("
                + "\"__fid__\" varchar NOT NULL, center varbinary, ellipse varbinary,"
                + " dtg timestamp(6) with time zone, label varchar,"
                + " \"__center_bbox__\" ROW(xmin real, ymin real, xmax real, ymax real),"
                + " \"__center_z2__\" varchar,"
                + " \"__ellipse_bbox__\" ROW(xmin real, ymin real, xmax real, ymax real),"
                + " \"__ellipse_xz2__\" varchar)"
                + " WITH (partitioning = ARRAY['truncate(__center_z2__, 2)',"
                + "'truncate(__ellipse_xz2__, 4)','month(dtg)'])");
            List<String> tuples = new ArrayList<>();
            for (String[] r : readFixture("/fixtures/observations_2geom.tsv")) {
                // __fid__, center_wkb_hex, ellipse_wkb_hex, dtg, label,
                // center bbox ×4, center_z2_hex, ellipse bbox ×4, ellipse_xz2_hex
                tuples.add("(" + str(r[0]) + ", from_hex(" + str(r[1]) + "),"
                    + " from_hex(" + str(r[2]) + "), TIMESTAMP '" + r[3] + "', " + str(r[4]) + ","
                    + " " + bbox(r[5], r[6], r[7], r[8]) + ", " + str(r[9]) + ","
                    + " " + bbox(r[10], r[11], r[12], r[13]) + ", " + str(r[14]) + ")");
                flushBatch(s, "spatial.observations_2geom", tuples, false);
            }
            flushBatch(s, "spatial.observations_2geom", tuples, true);
        }
    }

    // ── Shared helpers ────────────────────────────────────────────────────────

    private static String bbox(String xmin, String ymin, String xmax, String ymax) {
        return "CAST(ROW(" + xmin + ", " + ymin + ", " + xmax + ", " + ymax + ")"
            + " AS ROW(xmin real, ymin real, xmax real, ymax real))";
    }

    /** Renders the visibility expression for a fixture tier index: 0 → NULL,
     *  1 → the partial token, 2 → every full-set token ANDed. */
    private static String visExpr(String tier) {
        return switch (tier) {
            case "0" -> "NULL";
            case "1" -> str(PARTIAL_AUTH);
            case "2" -> str(String.join("&", FULL_AUTHS.split(",")));
            default -> throw new IllegalArgumentException("Unexpected vis tier: " + tier);
        };
    }

    /** SQL string literal with single quotes doubled; {@code \N} → NULL. */
    private static String str(String v) {
        return "\\N".equals(v) ? "NULL" : "'" + v.replace("'", "''") + "'";
    }

    /** Emits a batched INSERT when the buffer is full (or on the final flush).
     *  Each INSERT is its own snapshot, writing one file per touched partition —
     *  so the provisioned table has a multi-file, multi-partition layout like
     *  an ingested one. */
    private static void flushBatch(Statement s, String table, List<String> tuples, boolean force)
            throws SQLException {
        if (tuples.isEmpty() || (!force && tuples.size() < BATCH_ROWS)) {
            return;
        }
        s.execute("INSERT INTO " + table + " VALUES " + String.join(", ", tuples));
        tuples.clear();
    }

    /** Reads a bundled fixture TSV (header skipped) into per-row field arrays. */
    private static List<String[]> readFixture(String resource) throws Exception {
        InputStream in = TestFixtures.class.getResourceAsStream(resource);
        if (in == null) {
            throw new IllegalStateException("Missing bundled fixture " + resource);
        }
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String header = br.readLine();
            if (header == null) throw new IllegalStateException("Empty fixture " + resource);
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.isBlank()) rows.add(line.split("\t", -1));
            }
        }
        return rows;
    }
}
