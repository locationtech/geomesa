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
import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Schema drift: a table is dropped and recreated (or altered) underneath a
 * long-lived datastore. In particular, when the recreated table no longer has a
 * visibility column, queries must NOT keep SELECTing the cached {@code __vis__}
 * (COLUMN_NOT_FOUND) — the source refreshes its cached schema and retries.
 *
 * <p>DDL goes through the stock {@code iceberg} catalog (the spatial connector
 * is read-only); reads go through {@code spatial_iceberg}.
 */
@Tag("integration")
class SchemaDriftIT {

    private static final String TABLE = "schema_drift_it";
    private static final String DDL_URL = "jdbc:trino://localhost:8080/iceberg/spatial";

    private static DataStore ds;

    private static void ddl(String... statements) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "geomesa");
        try (Connection conn = DriverManager.getConnection(DDL_URL, props);
             Statement st = conn.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }

    @BeforeAll
    static void setup() throws Exception {
        try (var socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress("localhost", 8080), 2000);
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Trino not reachable at localhost:8080 — skipping integration tests");
        }

        // The marked row's visibility and the store's auths are configurable so the
        // test runs against any deployment's token scheme (same properties as
        // VisibilityIT). The Trino-layer access control also filters this catalog
        // using the deployment's auth mapping for the connecting service user, so
        // the partial token must be one that mapping covers.
        String partial = System.getProperty("geomesa.it.auths.partial", "basic");
        String full    = System.getProperty("geomesa.it.auths.full", "basic,privileged");
        ddl("CREATE SCHEMA IF NOT EXISTS spatial",
            "DROP TABLE IF EXISTS " + TABLE,
            "CREATE TABLE " + TABLE + " (\"__fid__\" varchar, name varchar, \"__vis__\" varchar)",
            "INSERT INTO " + TABLE + " VALUES ('1', 'a', '" + partial + "'), ('2', 'b', NULL)");

        Map<String, Object> params = new HashMap<>();
        params.put("trino.host",    "localhost");
        params.put("trino.port",    8080);
        params.put("trino.catalog", "spatial_iceberg");
        params.put("trino.schema",  "spatial");
        params.put("geomesa.security.auths", full);
        ds = DataStoreFinder.getDataStore(params);
        assertThat(ds).isNotNull();
    }

    @AfterAll
    static void teardown() throws Exception {
        if (ds != null) ds.dispose();
        ddl("DROP TABLE IF EXISTS " + TABLE);
    }

    private static int readAll() throws Exception {
        int n = 0;
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                 ds.getFeatureReader(new Query(TABLE), Transaction.AUTO_COMMIT)) {
            while (reader.hasNext()) { reader.next(); n++; }
        }
        return n;
    }

    @Test
    void recreatedTableWithoutVisColumnSelfHeals() throws Exception {
        // Prime the schema cache while the table HAS __vis__.
        SimpleFeatureType sft = ds.getSchema(TABLE);
        assertThat(sft.getUserData().get(TrinoSchemaDiscovery.VIS_COLUMN_KEY)).isEqualTo("__vis__");
        assertThat(readAll()).isEqualTo(2);

        // Recreate the table WITHOUT the vis column, underneath the live store.
        ddl("DROP TABLE " + TABLE,
            "CREATE TABLE " + TABLE + " (\"__fid__\" varchar, name varchar)",
            "INSERT INTO " + TABLE + " VALUES ('1', 'a'), ('2', 'b'), ('3', 'c')");

        // Pre-fix this failed with COLUMN_NOT_FOUND on the cached "__vis__";
        // the source must refresh its schema and stop enforcing visibility.
        assertThat(readAll()).isEqualTo(3);
        assertThat(ds.getFeatureSource(TABLE).getCount(Query.ALL)).isEqualTo(3);
    }
}
