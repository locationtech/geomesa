/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves row-visibility entitlements are enforced at the Trino layer — for
 * direct JDBC/SQL consumers, WITHOUT going through the GeoTools datastore — by
 * the connector's {@link VisibilityAccessControl}.
 *
 * <p>Pre-conditions (run from geomesa-trino/): {@code make up-trino} (deploys the
 * plugin with the access control) and {@code make ingest-demo-data} (observations
 * carries the U//FOUO {@code visibilities} ladder). The demo auth mapping
 * (config/trino/geomesa-auth-mapping.properties) grants {@code alice=U,FOUO},
 * {@code bob=U}; unmapped users get no auths.
 *
 * <p>Local Trino has no authentication, so the JDBC {@code user} property sets the
 * session identity. Assertions are monotonic inequalities (robust to row-count
 * variation), mirroring the datastore VisibilityIT.
 */
@Tag("integration")
class VisibilityEnforcementIT {

    private static final String SPATIAL = "jdbc:trino://localhost:8080/spatial_iceberg";
    private static final String PLAIN   = "jdbc:trino://localhost:8080/iceberg";

    @BeforeAll
    static void requiresTrino() {
        try (var socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress("localhost", 8080), 2000);
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Trino not reachable at localhost:8080 — skipping");
        }
    }

    private static long count(String jdbcBase, String user, String table) throws SQLException {
        try (Connection c = DriverManager.getConnection(jdbcBase + "?user=" + user);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM spatial." + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    // User identities below must exist in config/trino/auth/geomesa-auth-mapping.properties
    // + groups.txt: "public"=U (direct), "cwdobbi" in the fouo group → U,FOUO.
    @Test
    void directSqlIsRowFilteredByClearanceOnSpatialCatalog() throws SQLException {
        long nobody  = count(SPATIAL, "nobody",  "observations"); // unmapped → unrestricted only
        long publik  = count(SPATIAL, "public",  "observations"); // U (direct mapping)
        long cwdobbi = count(SPATIAL, "cwdobbi", "observations"); // U,FOUO (via fouo group)
        long admin   = count(SPATIAL, "admin",   "observations"); // unmapped → unrestricted only

        assertThat(nobody).as("unmapped user sees only unrestricted rows").isGreaterThan(0);
        assertThat(publik).as("U clearance sees more than unrestricted-only").isGreaterThan(nobody);
        assertThat(cwdobbi).as("group-granted U,FOUO sees more than U-only").isGreaterThan(publik);
        assertThat(admin).as("another unmapped user matches the unmapped baseline").isEqualTo(nobody);
    }

    @Test
    void nonVisibilityTableIsUnaffected() throws SQLException {
        // regions (also created by make ingest-demo-data) has no visibilities
        // column → no filter emitted, no error, same count for every identity.
        long asNobody  = count(SPATIAL, "nobody",  "regions");
        long asCwdobbi = count(SPATIAL, "cwdobbi", "regions");
        assertThat(asNobody).isGreaterThan(0);
        assertThat(asCwdobbi).isEqualTo(asNobody);
    }

    @Test
    void plainIcebergCatalogBypassesEnforcementWhenExposed() throws SQLException {
        // Documents the known boundary: the plain `iceberg` catalog is not wrapped
        // by SpatialConnector, so it is NOT row-filtered. Hardened deployments
        // close this hole by not exposing the plain catalog at all (recommended)
        // — in that case there is nothing to bypass and this test is moot.
        long viaPlainNobody;
        try {
            viaPlainNobody = count(PLAIN, "nobody", "observations");
        } catch (SQLException e) {
            Assumptions.assumeTrue(false,
                "plain iceberg catalog not exposed — bypass already closed (" + e.getMessage() + ")");
            return;
        }
        long viaSpatialNobody = count(SPATIAL, "nobody", "observations");
        assertThat(viaPlainNobody)
            .as("plain iceberg catalog returns ALL rows (unfiltered escape hatch)")
            .isGreaterThan(viaSpatialNobody);
    }
}
