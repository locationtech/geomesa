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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ship gate for connector-side bbox-short-circuit: with {@code geomesa.spatial.bbox-page-filter=true},
 * the connector claims rectangle {@code ST_Intersects} on point columns <em>enforced</em> and filters
 * in {@code BboxFilteringPageSource} instead of the engine — so there is no exact-predicate safety
 * net, and only a corpus check against the engine's own {@code ST_Intersects} proves it correct.
 *
 * <p>Two invariants, on the {@code spatial.observations} demo table (points, z2 → bbox-short-circuit
 * eligible, and carrying {@code __vis__}):
 * <ol>
 *   <li><strong>Spatial parity</strong> — for every rectangle in the corpus (including whole-world,
 *       empty, and mid-extent boxes whose edges straddle data and exercise the exact shell test),
 *       the count via {@code spatial_iceberg} as a full-auth identity equals the count via the plain
 *       {@code iceberg} connector (which evaluates the exact {@code ST_Intersects} with no
 *       visibility). Any divergence means bbox-short-circuit dropped or admitted a row the engine didn't.</li>
 *   <li><strong>Visibility preserved</strong> — a partial-auth identity sees strictly fewer rows than
 *       a full-auth identity, proving the {@code is_visible} row-filter still applies under
 *       bbox-short-circuit. (Regression guard: the first cut replaced the whole residual with TRUE and
 *       silently bypassed visibility — full-auth parity alone would not have caught it.)</li>
 * </ol>
 *
 * <p>Requires a running Trino at localhost:8080 with the flag ON; skips otherwise. Relies on the
 * local file auth-mapping (user {@code geomesa} → full ladder; {@code public} → partial tier).
 */
@Tag("integration")
class BboxShortCircuitParityIT {

    private static final String SPATIAL = "jdbc:trino://localhost:8080/spatial_iceberg";
    private static final String PLAIN   = "jdbc:trino://localhost:8080/iceberg";

    /** Full-auth identity (sees every visibility tier the fixture uses). */
    private static final String FULL_USER    = "geomesa";
    /** Partial-auth identity (a strict subset of the tiers). */
    private static final String PARTIAL_USER = "public";

    private static boolean ready;

    /** Rectangles spanning the whole world down to sub-degree boxes at varied positions; the exact
     *  result is unknown but must be identical between the two engines for every one. */
    private static final List<String> BOXES = List.of(
        "POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))",     // whole world → all rows
        "POLYGON ((-180 -90, -170 -90, -170 -80, -180 -80, -180 -90))", // far corner → likely empty
        "POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))",
        "POLYGON ((0 0, 40 0, 40 40, 0 40, 0 0))",
        "POLYGON ((-100 20, -80 20, -80 40, -100 40, -100 20))",
        "POLYGON ((-77 38, -77 40, -76 40, -76 38, -77 38))",
        "POLYGON ((-0.5 -0.5, 0.5 -0.5, 0.5 0.5, -0.5 0.5, -0.5 -0.5))",
        "POLYGON ((30 30, 30.25 30, 30.25 30.25, 30 30.25, 30 30))");

    @BeforeAll
    static void setup() {
        try (var socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress("localhost", 8080), 2000);
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Trino not reachable at localhost:8080 — skipping");
        }
        ready = TestFixtures.ensureTable("observations");
        Assumptions.assumeTrue(ready, "spatial.observations unavailable — skipping");
    }

    private static long count(String catalogUrl, String user, String box) throws SQLException {
        String sql = "SELECT count(*) FROM spatial.observations"
            + " WHERE ST_Intersects(ST_GeometryFromText('" + box + "'), ST_GeomFromBinary(geom))";
        try (Connection c = DriverManager.getConnection(catalogUrl + "?user=" + user);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void bboxShortCircuitMatchesEngineExactAcrossBoxes() throws SQLException {
        for (String box : BOXES) {
            long viaConnector = count(SPATIAL, FULL_USER, box);   // bbox-short-circuit, all rows visible
            long viaEngine    = count(PLAIN, "admin", box);       // exact ST_Intersects, no visibility
            assertThat(viaConnector)
                .as("bbox-short-circuit count must equal the engine's exact ST_Intersects for %s", box)
                .isEqualTo(viaEngine);
        }
    }

    @Test
    void visibilityStillEnforcedUnderBboxShortCircuit() throws SQLException {
        String all = BOXES.get(0);
        long full    = count(SPATIAL, FULL_USER, all);
        long partial = count(SPATIAL, PARTIAL_USER, all);
        assertThat(full)
            .as("full-auth identity must see the fixture's restricted rows")
            .isGreaterThan(0);
        assertThat(partial)
            .as("partial-auth identity must see strictly fewer rows — is_visible must survive "
                + "bbox-short-circuit (regression guard for the visibility-bypass bug)")
            .isLessThan(full);
    }
}
