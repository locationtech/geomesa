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
 * Multi-geometry-column IT. Requires a running Trino at localhost:8080 with the
 * plugin loaded and the demo table spatial.observations_2geom ingested, with
 * center (Z2-partitioned) and ellipse (XZ2-partitioned) geometry columns.
 * Skips (JUnit assumption) when Trino is unreachable or the table is absent.
 *
 * Skipped by default; run with -DskipITs=false (matching Z2PruningIT).
 */
@Tag("integration")
class MultiGeomIT {

    private static final String JDBC_URL =
        "jdbc:trino://localhost:8080/spatial_iceberg?user=admin";

    private static Connection conn;

    @BeforeAll
    static void connect() {
        try {
            conn = DriverManager.getConnection(JDBC_URL);
            conn.createStatement().executeQuery("SELECT 1").close();
        } catch (SQLException e) {
            Assumptions.assumeTrue(false,
                "Trino not reachable at localhost:8080 — skipping (" + e.getMessage() + ")");
        }
        Assumptions.assumeTrue(TestFixtures.ensureTable("observations_2geom"),
            "spatial.observations_2geom not ingested and not provisionable — skipping (see class javadoc)");
    }

    @Test
    void describeShowsBothGeomColumnsAsVarbinary() throws SQLException {
        // Both geometry columns surface as VARBINARY (raw WKB) — there is no
        // Geometry-type overlay; spatial SQL wraps them with ST_GeomFromBinary.
        String centerType = null, ellipseType = null;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("DESCRIBE spatial_iceberg.spatial.observations_2geom")) {
            while (rs.next()) {
                String name = rs.getString(1);
                if ("center".equals(name))  centerType  = rs.getString(2);
                if ("ellipse".equals(name)) ellipseType = rs.getString(2);
            }
        }
        assertThat(centerType).isEqualToIgnoringCase("varbinary");
        assertThat(ellipseType).isEqualToIgnoringCase("varbinary");
    }

    @Test
    void describeShowsBothPartitionColumns() throws SQLException {
        String z2 = null, xz2 = null;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("DESCRIBE spatial_iceberg.spatial.observations_2geom")) {
            while (rs.next()) {
                String name = rs.getString(1);
                if ("__center_z2__".equals(name))   z2  = rs.getString(2);
                if ("__ellipse_xz2__".equals(name)) xz2 = rs.getString(2);
            }
        }
        // After the hex-encoded migration both partition columns are varchar.
        assertThat(z2).isEqualToIgnoringCase("varchar");
        assertThat(xz2).isEqualToIgnoringCase("varchar");
    }

    @Test
    void bothPartitionPushdownsReduceScannedRows() throws SQLException {
        // Trino's iceberg connector projects truncate-string partition
        // predicates at split time but does NOT surface them in EXPLAIN's
        // `constraint on [...]` block. Verify pruning via EXPLAIN ANALYZE
        // scan-input row count: with both geoms predicated, the connector
        // pushes Z2 ranges on __center_z2__ AND XZ2 ranges on __ellipse_xz2__,
        // so the scan input must be strictly less than the table total.
        String polygon = "POLYGON((-90 35,-80 35,-80 45,-90 45,-90 35))";
        long totalRows = scalarLong("SELECT COUNT(*) FROM spatial_iceberg.spatial.observations_2geom");
        long scannedRows = scanInputRows(
            "SELECT COUNT(*) FROM spatial_iceberg.spatial.observations_2geom " +
            "WHERE ST_Intersects(ST_GeomFromBinary(center),  ST_GeometryFromText('" + polygon + "')) " +
            "AND   ST_Intersects(ST_GeomFromBinary(ellipse), ST_GeometryFromText('" + polygon + "'))");
        assertThat(scannedRows)
            .as("dual-geom predicate must reduce scanned rows below total (%d)", totalRows)
            .isLessThan(totalRows);
        assertThat(scannedRows).isPositive();
    }

    @Test
    void singleGeomQueryPushesOnlyItsOwnPartition() throws SQLException {
        // Both single-geom variants must independently prune: scan input must
        // shrink relative to total when each geom is predicated alone, and the
        // two reductions cover different partition prefixes (so their scan
        // inputs are not identical — that would imply only one pushdown fires).
        String polygon = "POLYGON((-90 35,-80 35,-80 45,-90 45,-90 35))";
        long totalRows = scalarLong("SELECT COUNT(*) FROM spatial_iceberg.spatial.observations_2geom");
        long centerScanned = scanInputRows(
            "SELECT COUNT(*) FROM spatial_iceberg.spatial.observations_2geom " +
            "WHERE ST_Intersects(ST_GeomFromBinary(center), ST_GeometryFromText('" + polygon + "'))");
        long ellipseScanned = scanInputRows(
            "SELECT COUNT(*) FROM spatial_iceberg.spatial.observations_2geom " +
            "WHERE ST_Intersects(ST_GeomFromBinary(ellipse), ST_GeometryFromText('" + polygon + "'))");
        assertThat(centerScanned)
            .as("center-only predicate must prune below total (%d)", totalRows)
            .isLessThan(totalRows).isPositive();
        assertThat(ellipseScanned)
            .as("ellipse-only predicate must prune below total (%d)", totalRows)
            .isLessThan(totalRows).isPositive();
    }

    @Test
    void countMatchesUnprunedBaseline() throws SQLException {
        // Same correctness cross-check as Z2PruningIT.regionsCountMatchesUnprunedBaseline,
        // but with predicates on TWO geom columns. The spatial_iceberg side prunes via
        // both partitions; the iceberg (baseline) side scans every row. Counts must match.
        String centerPolygon  = "POLYGON((-100 30,-70 30,-70 50,-100 50,-100 30))";
        String ellipsePolygon = "POLYGON((-100 30,-70 30,-70 50,-100 50,-100 30))";

        long spatial;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT COUNT(*) FROM spatial_iceberg.spatial.observations_2geom " +
                "WHERE ST_Intersects(ST_GeomFromBinary(center),  ST_GeometryFromText('" + centerPolygon  + "')) " +
                "AND   ST_Intersects(ST_GeomFromBinary(ellipse), ST_GeometryFromText('" + ellipsePolygon + "'))")) {
            rs.next();
            spatial = rs.getLong(1);
        }
        long baseline;
        try (Connection iceberg = DriverManager.getConnection(
                "jdbc:trino://localhost:8080/iceberg?user=admin");
             Statement s = iceberg.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT COUNT(*) FROM iceberg.spatial.observations_2geom " +
                "WHERE ST_Intersects(ST_GeomFromBinary(center),  ST_GeometryFromText('" + centerPolygon  + "')) " +
                "AND   ST_Intersects(ST_GeomFromBinary(ellipse), ST_GeometryFromText('" + ellipsePolygon + "'))")) {
            rs.next();
            baseline = rs.getLong(1);
        }
        assertThat(spatial).isEqualTo(baseline);
        assertThat(spatial)
            .as("count must be > 0 to actually prove the predicate fires")
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
