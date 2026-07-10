/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: requires a running Trino at localhost:8080 with the plugin
 * loaded and a spatial_iceberg catalog configured. Skipped by default; run with
 * -DskipITs=false.
 *
 * Pre-conditions — the following demo tables must be ingested:
 *   - spatial.regions (XZ2), required by the regions* tests
 *   - spatial.tdrive (Z2), required by every other test below
 *
 *   The tests themselves do not ingest — each test skips (JUnit assumption)
 *   when its dataset is absent, so a partially-provisioned Trino runs the
 *   tests it can and skips the rest.
 */
@Tag("integration")
class Z2PruningIT {

    private static final String TRINO_JDBC_URL =
        "jdbc:trino://localhost:8080/spatial_iceberg?user=admin";

    private static Connection conn;
    private static java.util.Set<String> tables = java.util.Set.of();

    @BeforeAll
    static void connect() {
        try {
            conn = DriverManager.getConnection(TRINO_JDBC_URL);
            java.util.Set<String> found = new java.util.HashSet<>();
            // information_schema, not SHOW TABLES: the latter throws when the
            // spatial schema doesn't exist yet (fresh warehouse) — that state
            // must fall through to per-test provisioning/skips, not abort here.
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT table_name FROM information_schema.tables"
                     + " WHERE table_schema = 'spatial'")) {
                while (rs.next()) found.add(rs.getString(1));
            }
            tables = found;
        } catch (SQLException e) {
            Assumptions.assumeTrue(false,
                "Trino not reachable at localhost:8080 — skipping (" + e.getMessage() + ")");
        }
    }

    /** Skips the calling test when the demo table is not ingested and cannot be
     *  provisioned from a bundled fixture (regions is; tdrive is not). */
    private static void assumeTable(String table) {
        Assumptions.assumeTrue(tables.contains(table) || TestFixtures.ensureTable(table),
            "spatial." + table + " not ingested and not provisionable — skipping (see class javadoc)");
    }

    @Test
    void explainPreservesSpatialFilterAsResidual() throws SQLException {
        assumeTable("tdrive");
        // Bbox sub-field domains ARE injected by SpatialConnectorMetadata (xmin/ymin/
        // xmax/ymax on __geom_bbox__), but Iceberg's EXPLAIN summary doesn't surface
        // nested struct sub-field constraints in `constraint on [...]` — only top-level
        // partition columns appear there (see explainShowsZ2PartitionFilter). The
        // visible invariant we CAN pin is residual-filter preservation: even after
        // pushing down Z2 + bbox domains, ST_Intersects must remain in filterPredicate
        // as the row-level safety net, because both bbox and Z2 ranges are supersets
        // of the actual intersecting rows.
        String explain;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                "EXPLAIN (TYPE DISTRIBUTED) " +
                "SELECT __fid__, geom FROM spatial_iceberg.spatial.tdrive " +
                "WHERE ST_Intersects(ST_GeomFromBinary(geom), " +
                "  ST_GeometryFromText('POLYGON((116 39, 117 39, 117 40, 116 40, 116 39))'))")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) sb.append(rs.getString(1)).append('\n');
            explain = sb.toString();
        }
        assertThat(explain)
            .as("ST_Intersects must remain as residual filterPredicate (row-level correctness)")
            .containsIgnoringCase("st_intersects");
    }

    @Test
    void z2PartitionPushdownReducesScannedRows() throws SQLException {
        assumeTable("tdrive");
        // Trino's iceberg connector projects truncate-string partition
        // predicates at split time but does NOT surface them in the EXPLAIN
        // summary's `constraint on [...]` block (that block only shows
        // identity-partitioned columns). The observable proof of pushdown is
        // that EXPLAIN ANALYZE reports a scan-input row count significantly
        // smaller than the table's total rows.
        long totalRows = scalarLong("SELECT COUNT(*) FROM spatial_iceberg.spatial.tdrive");
        long scannedRows = scanInputRows(
            "SELECT COUNT(*) FROM spatial_iceberg.spatial.tdrive " +
            "WHERE ST_Intersects(ST_GeomFromBinary(geom), " +
            "  ST_GeometryFromText('POLYGON((116 39, 117 39, 117 40, 116 40, 116 39))'))");
        assertThat(scannedRows)
            .as("Z2 partition pushdown must reduce scanned rows below total (%d)", totalRows)
            .isLessThan(totalRows);
        assertThat(scannedRows).isPositive();
    }

    @Test
    void geometryColumnTypeIsVarbinary() throws SQLException {
        assumeTable("tdrive");
        // No Geometry-type overlay: geom is plain VARBINARY (raw WKB). Spatial SQL
        // wraps it with ST_GeomFromBinary; the connector's value is planning-time
        // pushdown, not a column-type swap.
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                "SELECT data_type FROM spatial_iceberg.information_schema.columns " +
                "WHERE table_schema = 'spatial' AND table_name = 'tdrive' " +
                "AND column_name = 'geom'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualToIgnoringCase("varbinary");
        }
    }

    @Test
    void describeShowsGeomAsVarbinaryOnBothCatalogs() throws SQLException {
        assumeTable("tdrive");
        // Both catalogs expose identical column shapes — geom is VARBINARY on
        // spatial_iceberg and iceberg alike. The spatial connector adds no type
        // overlay; it only injects bbox/Z2 pushdown in applyFilter at planning time.
        for (String table : new String[]{
                "spatial_iceberg.spatial.tdrive", "iceberg.spatial.tdrive"}) {
            String url = table.startsWith("iceberg.")
                ? "jdbc:trino://localhost:8080/iceberg?user=admin"
                : TRINO_JDBC_URL;
            try (Connection c = DriverManager.getConnection(url);
                 Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery("DESCRIBE " + table)) {
                String geomType = null;
                while (rs.next()) {
                    if ("geom".equals(rs.getString(1))) {
                        geomType = rs.getString(2);
                        break;
                    }
                }
                assertThat(geomType)
                    .as("%s should report geom as varbinary", table)
                    .isEqualToIgnoringCase("varbinary");
            }
        }
    }

    @Test
    void stIntersectsCountMatchesBaseline() throws SQLException {
        assumeTable("tdrive");
        // Correctness cross-check on tdrive: the spatial_iceberg connector's bbox/Z2
        // pushdown must not change results vs the stock iceberg baseline. Both sides
        // wrap geom with ST_GeomFromBinary (the only valid form — geom is varbinary).
        String polygon = "POLYGON((116 39, 117 39, 117 40, 116 40, 116 39))";

        long spatial; long baseline;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM spatial.tdrive " +
                "WHERE ST_Intersects(ST_GeomFromBinary(geom), ST_GeometryFromText('" + polygon + "'))")) {
            rs.next();
            spatial = rs.getLong(1);
        }
        try (Connection icebergConn = DriverManager.getConnection(
                "jdbc:trino://localhost:8080/iceberg?user=admin");
             Statement stmt = icebergConn.createStatement();
             ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM spatial.tdrive " +
                "WHERE ST_Intersects(ST_GeomFromBinary(geom), ST_GeometryFromText('" + polygon + "'))")) {
            rs.next();
            baseline = rs.getLong(1);
        }
        assertThat(spatial).isEqualTo(baseline);
        assertThat(spatial).isGreaterThan(0);
    }

    @Test
    void miscSpatialFunctionsWork() throws SQLException {
        assumeTable("tdrive");
        // Smoke test for the stock geospatial functions on the WKB-backed geom column,
        // wrapped with ST_GeomFromBinary (Trino 481 requires the explicit wrap —
        // there is no implicit varbinary→geometry coercion).
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                "SELECT ST_AsText(ST_GeomFromBinary(geom)) FROM spatial.tdrive LIMIT 1")) {
                rs.next();
                assertThat(rs.getString(1)).startsWith("POINT (");
            }
            try (ResultSet rs = stmt.executeQuery(
                "SELECT ST_GeometryType(ST_GeomFromBinary(geom)) FROM spatial.tdrive LIMIT 1")) {
                rs.next();
                assertThat(rs.getString(1)).isEqualTo("ST_Point");
            }
            try (ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM spatial.tdrive " +
                "WHERE ST_Within(ST_GeomFromBinary(geom), ST_GeometryFromText('POLYGON((116 39, 117 39, 117 40, 116 40, 116 39))'))")) {
                rs.next();
                assertThat(rs.getLong(1)).isGreaterThan(0);
            }
            // ST_Contains — polygon contains the points
            try (ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM spatial.tdrive " +
                "WHERE ST_Contains(ST_GeometryFromText('POLYGON((116 39, 117 39, 117 40, 116 40, 116 39))'), ST_GeomFromBinary(geom))")) {
                rs.next();
                assertThat(rs.getLong(1)).isGreaterThan(0);
            }
            // ST_Distance via to_spherical_geography — the actual production path
            // used by DWITHIN's distance check.
            try (ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM spatial.tdrive " +
                "WHERE ST_Distance(to_spherical_geography(ST_GeomFromBinary(geom)), " +
                "to_spherical_geography(ST_GeometryFromText('POINT(116.3912 39.9075)'))) <= 1000")) {
                rs.next();
                assertThat(rs.getLong(1)).isGreaterThanOrEqualTo(0);  // smoke; just must not throw
            }
            // DWITHIN-style (same shape as TrinoFilterToSQL emits for non-shortcut path)
            try (ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM spatial.tdrive " +
                "WHERE ST_Distance(to_spherical_geography(ST_GeomFromBinary(geom)), " +
                "to_spherical_geography(ST_GeometryFromText('POINT(116.3912 39.9075)'))) <= 50000")) {
                rs.next();
                assertThat(rs.getLong(1)).isGreaterThan(0);
            }
        }
    }

    @Test
    void regionsDescribeShowsXz2Column() throws SQLException {
        assumeTable("regions");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("DESCRIBE spatial_iceberg.spatial.regions")) {
            String xz2 = null;
            String z2 = null;
            while (rs.next()) {
                String name = rs.getString(1);
                if ("__geom_xz2__".equals(name)) xz2 = rs.getString(2);
                else if ("__geom_z2__".equals(name)) z2 = rs.getString(2);
            }
            assertThat(xz2)
                .as("regions table must expose __geom_xz2__ as varchar after the hex-encoded migration")
                .isEqualToIgnoringCase("varchar");
            assertThat(z2)
                .as("regions table must NOT expose __geom_z2__ — mutual exclusion")
                .isNull();
        }
    }

    @Test
    void regionsXz2PartitionPushdownReducesScannedRows() throws SQLException {
        assumeTable("regions");
        // Trino's iceberg connector projects truncate-string partition
        // predicates at split time but does NOT surface them in the EXPLAIN
        // summary's `constraint on [...]` block. The observable proof of XZ2
        // pushdown is that EXPLAIN ANALYZE reports a scan-input row count
        // significantly smaller than the table's total rows.
        String polygon = "POLYGON((-80 35, -70 35, -70 45, -80 45, -80 35))";
        long totalRows = scalarLong("SELECT COUNT(*) FROM spatial_iceberg.spatial.regions");
        long scannedRows = scanInputRows(
            "SELECT COUNT(*) FROM spatial_iceberg.spatial.regions " +
            "WHERE ST_Intersects(ST_GeomFromBinary(geom), ST_GeometryFromText('" + polygon + "'))");
        assertThat(scannedRows)
            .as("XZ2 partition pushdown must reduce scanned rows below total (%d)", totalRows)
            .isLessThan(totalRows);
        assertThat(scannedRows).isPositive();
    }

    @Test
    void regionsCountMatchesUnprunedBaseline() throws SQLException {
        assumeTable("regions");
        // Cross-check correctness: count rows on spatial_iceberg.spatial.regions with
        // the XZ2 partition predicate active matches the count without any predicate
        // applied at the iceberg (baseline) catalog. Same physical Parquet files,
        // different connector → if XZ2 had a false-negative, the count would be lower.
        String polygon = "POLYGON((-80 35, -70 35, -70 45, -80 45, -80 35))";

        long spatial;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM spatial_iceberg.spatial.regions " +
                "WHERE ST_Intersects(ST_GeomFromBinary(geom), ST_GeometryFromText('" + polygon + "'))")) {
            rs.next();
            spatial = rs.getLong(1);
        }

        long baseline;
        try (Connection icebergConn = DriverManager.getConnection(
                "jdbc:trino://localhost:8080/iceberg?user=admin");
             Statement stmt = icebergConn.createStatement();
             ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM iceberg.spatial.regions " +
                "WHERE ST_Intersects(ST_GeomFromBinary(geom), ST_GeometryFromText('" + polygon + "'))")) {
            rs.next();
            baseline = rs.getLong(1);
        }

        assertThat(spatial).isEqualTo(baseline);
        assertThat(spatial)
            .as("regions count must be > 0 to prove anything — a zero on both sides would silently pass")
            .isGreaterThan(0);
    }

    private long scalarLong(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** Parse the "Input: N rows" field reported under the ScanFilterProject node
     *  of EXPLAIN ANALYZE — the only reliable proxy for "did Iceberg prune at
     *  split time" when truncate-string transforms hide the projection from the
     *  EXPLAIN summary's `constraint on [...]` block. */
    private long scanInputRows(String selectQuery) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN ANALYZE " + selectQuery)) {
            while (rs.next()) plan.append(rs.getString(1)).append('\n');
        }
        java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("Input:\\s+(\\d+)\\s+rows")
            .matcher(plan.toString());
        if (!m.find()) {
            throw new AssertionError(
                "Could not find 'Input: N rows' in EXPLAIN ANALYZE output:\n" + plan);
        }
        return Long.parseLong(m.group(1));
    }
}
