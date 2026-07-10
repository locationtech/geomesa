/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

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
 * values — bbox floats, hex-encoded Z2 cells — match real writer output
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
final class TestFixtures {

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
    static boolean ensureTable(String table) {
        try {
            if (tableExists(table)) {
                return true;
            }
            if (!"observations".equals(table)) {
                return false;  // no bundled fixture for this table in this module
            }
            provisionObservations();
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
                    + " CAST(ROW(" + r[7] + ", " + r[8] + ", " + r[9] + ", " + r[10] + ")"
                    + " AS ROW(xmin real, ymin real, xmax real, ymax real)), " + str(r[11]) + ")");
                flushBatch(s, "spatial.observations", tuples, false);
            }
            flushBatch(s, "spatial.observations", tuples, true);
        }
    }

    // ── Shared helpers ────────────────────────────────────────────────────────

    /**
     * Resolves the {@code [partialAuth, fullAuthsCsv]} visibility ladder the
     * visibility ITs should query with. Explicit
     * {@code -Dgeomesa.it.auths.partial}/{@code .full} properties win; otherwise
     * the ladder is DETECTED from the table's distinct non-null {@code __vis__}
     * values (via the plain catalog, which is not row-filtered): the
     * single-token value is the partial auth, the {@code &}-joined value split
     * to CSV is the full set. Falls back to the generic defaults when neither
     * is available (e.g. plain catalog not exposed). Detection makes the tests
     * flag-free against any deployment's token scheme — the data defines the
     * ladder, and freshly-provisioned fixtures are written from the same
     * properties/defaults, so the two always agree.
     */
    static String[] visibilityLadder(String table) {
        String partial = System.getProperty("geomesa.it.auths.partial");
        String full = System.getProperty("geomesa.it.auths.full");
        if (partial == null || full == null) {
            try (Connection c = DriverManager.getConnection(PLAIN_URL);
                 Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT DISTINCT \"__vis__\" FROM spatial."
                     + table + " WHERE \"__vis__\" IS NOT NULL")) {
                while (rs.next()) {
                    String vis = rs.getString(1);
                    if (vis.contains("&")) {
                        if (full == null) full = String.join(",", vis.split("&"));
                    } else if (partial == null) {
                        partial = vis;
                    }
                }
            } catch (SQLException e) {
                System.err.println("TestFixtures: could not detect visibility ladder from spatial."
                    + table + " — " + e.getMessage());
            }
        }
        return new String[]{
            partial != null ? partial : PARTIAL_AUTH,
            full != null ? full : FULL_AUTHS};
    }

    /** Renders the visibility expression for a fixture tier index: 0 → NULL,
     *  1 → the partial token, 2 → every full-set token ANDed. */
    static String visExpr(String tier) {
        return switch (tier) {
            case "0" -> "NULL";
            case "1" -> str(PARTIAL_AUTH);
            case "2" -> str(String.join("&", FULL_AUTHS.split(",")));
            default -> throw new IllegalArgumentException("Unexpected vis tier: " + tier);
        };
    }

    /** SQL string literal with single quotes doubled; {@code \N} → NULL. */
    static String str(String v) {
        return "\\N".equals(v) ? "NULL" : "'" + v.replace("'", "''") + "'";
    }

    /** Emits a batched INSERT when the buffer is full (or on the final flush).
     *  Each INSERT is its own snapshot, writing one file per touched partition —
     *  so the provisioned table has a multi-file, multi-partition layout like
     *  an ingested one. */
    static void flushBatch(Statement s, String table, List<String> tuples, boolean force)
            throws SQLException {
        if (tuples.isEmpty() || (!force && tuples.size() < BATCH_ROWS)) {
            return;
        }
        s.execute("INSERT INTO " + table + " VALUES " + String.join(", ", tuples));
        tuples.clear();
    }

    /** Reads a bundled fixture TSV (header skipped) into per-row field arrays. */
    static List<String[]> readFixture(String resource) throws Exception {
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
