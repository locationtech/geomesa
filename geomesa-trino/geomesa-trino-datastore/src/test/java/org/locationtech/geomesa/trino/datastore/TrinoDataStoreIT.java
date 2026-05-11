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
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class TrinoDataStoreIT {

    private static DataStore ds;
    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    @BeforeAll
    static void connect() throws Exception {
        try (var socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress("localhost", 8080), 2000);
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Trino not reachable at localhost:8080 — skipping integration tests");
        }

        Map<String, Object> params = Map.of(
            "host",           "localhost",
            "port",           8080,
            "catalog",        "spatial_iceberg",
            "schema",         "spatial",
            "icebergRestUrl", "http://localhost:8181"
        );
        ds = DataStoreFinder.getDataStore(params);
        assertThat(ds).as("DataStoreFinder must locate TrinoDataStoreFactory").isNotNull();
    }

    @AfterAll
    static void disconnect() {
        if (ds != null) ds.dispose();
    }

    // ── Schema discovery ──────────────────────────────────────────────────────

    @Test
    void getNamesIncludesObservations() throws Exception {
        List<String> names = Arrays.stream(ds.getTypeNames()).toList();
        assertThat(names).contains("observations");
    }

    @Test
    void observationsSchemaHasCorrectAttributes() throws Exception {
        SimpleFeatureType sft = ds.getSchema("observations");
        List<String> attrNames = sft.getAttributeDescriptors().stream()
            .map(AttributeDescriptor::getLocalName).toList();
        assertThat(attrNames).contains("geom", "dtg", "sensor_id", "value", "active");
        assertThat(attrNames).doesNotContain("__geom_bbox__", "__fid__");
    }

    @Test
    void observationsDefaultGeometryIsGeom() throws Exception {
        SimpleFeatureType sft = ds.getSchema("observations");
        assertThat(sft.getGeometryDescriptor().getLocalName()).isEqualTo("geom");
        assertThat(Geometry.class)
            .isAssignableFrom(sft.getGeometryDescriptor().getType().getBinding());
    }

    // ── Per-filter count verification ─────────────────────────────────────────
    // Each test: run FeatureReader with CQL filter, assert count matches
    // direct JDBC COUNT(*) with equivalent SQL.

    @Test
    void bboxFilterCountMatchesBaseline() throws Exception {
        Filter f = FF.bbox("geom", -80.0, 37.0, -70.0, 45.0, "EPSG:4326");
        assertCountMatchesJdbc(f,
            "WHERE \"__geom_bbox__\".xmax >= -80.0 AND \"__geom_bbox__\".xmin <= -70.0" +
            " AND \"__geom_bbox__\".ymax >= 37.0 AND \"__geom_bbox__\".ymin <= 45.0");
    }

    @Test
    void intersectsFilterCountMatchesBaseline() throws Exception {
        Filter f = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        assertCountMatchesJdbc(f,
            "WHERE ST_Intersects(ST_GeomFromBinary(geom)," +
            " ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'))");
    }

    @Test
    void withinFilterCountMatchesBaseline() throws Exception {
        Filter f = ECQL.toFilter(
            "WITHIN(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        assertCountMatchesJdbc(f,
            "WHERE ST_Within(ST_GeomFromBinary(geom)," +
            " ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'))");
    }

    @Test
    void dwithinFilterCountMatchesBaseline() throws Exception {
        Filter f = ECQL.toFilter(
            "DWITHIN(geom, POINT(-77.0369 38.9072), 100000, meters)");
        long dsCount = countViaDataStore(f);
        long jdbcCount = countViaJdbc(
            "WHERE ST_Distance(" +
            "to_spherical_geography(ST_GeomFromBinary(geom))," +
            "to_spherical_geography(ST_GeometryFromText('POINT (-77.0369 38.9072)'))) <= 100000");
        assertThat(dsCount).isEqualTo(jdbcCount);
    }

    @Test
    void duringFilterCountMatchesBaseline() throws Exception {
        Filter f = ECQL.toFilter(
            "dtg DURING 2023-01-01T00:00:00Z/2024-01-01T00:00:00Z");
        assertCountMatchesJdbc(f,
            "WHERE dtg > TIMESTAMP '2023-01-01 00:00:00 UTC'" +
            " AND dtg < TIMESTAMP '2024-01-01 00:00:00 UTC'");
    }

    @Test
    void intersectsAndDuringCountMatchesBaseline() throws Exception {
        Filter f = ECQL.toFilter(
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))" +
            " AND dtg DURING 2023-01-01T00:00:00Z/2024-01-01T00:00:00Z");
        assertCountMatchesJdbc(f,
            "WHERE ST_Intersects(ST_GeomFromBinary(geom)," +
            " ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'))" +
            " AND dtg > TIMESTAMP '2023-01-01 00:00:00 UTC'" +
            " AND dtg < TIMESTAMP '2024-01-01 00:00:00 UTC'");
    }

    @Test
    void fidInFilterCountMatchesBaseline() throws Exception {
        List<String> fids = fetchFids(5);
        String fidList = fids.stream()
            .map(s -> "'" + s.replace("'", "''") + "'")
            .collect(Collectors.joining(","));
        Set<org.geotools.api.filter.identity.FeatureId> ids = fids.stream()
            .map(FF::featureId)
            .collect(Collectors.toSet());
        Filter f = FF.id(ids);
        assertCountMatchesJdbc(f, "WHERE \"__fid__\" IN (" + fidList + ")");
    }

    @Test
    void attributeAndIntersectsCountMatchesBaseline() throws Exception {
        Filter f = ECQL.toFilter(
            "active = TRUE AND value > 50.0 AND " +
            "INTERSECTS(geom, POLYGON((-80 37, -70 37, -70 45, -80 45, -80 37)))");
        assertCountMatchesJdbc(f,
            "WHERE active = TRUE AND value > 50.0" +
            " AND ST_Intersects(ST_GeomFromBinary(geom)," +
            " ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'))");
    }

    // ── getCount / getBounds ──────────────────────────────────────────────────

    @Test
    void getCountReturnsCorrectTotal() throws Exception {
        long jdbcTotal = countViaJdbc("");
        Query q = new Query("observations", Filter.INCLUDE);
        int count = ds.getFeatureSource("observations").getCount(q);
        assertThat((long) count).isEqualTo(jdbcTotal);
    }

    @Test
    void getBoundsReturnsContinentalUsBoundingBox() throws Exception {
        Query q = new Query("observations", Filter.INCLUDE);
        var bounds = ds.getFeatureSource("observations").getBounds(q);
        assertThat(bounds).isNotNull();
        // Synthetic data spans CONUS: roughly -125..−66 lon, 24..49 lat
        assertThat(bounds.getMinX()).isLessThan(-100);
        assertThat(bounds.getMaxX()).isGreaterThan(-80);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void assertCountMatchesJdbc(Filter filter, String whereClause) throws Exception {
        long dsCount   = countViaDataStore(filter);
        long jdbcCount = countViaJdbc(whereClause);
        assertThat(dsCount)
            .as("DataStore count should match direct JDBC count for filter: " + filter)
            .isEqualTo(jdbcCount);
    }

    private long countViaDataStore(Filter filter) throws Exception {
        long count = 0;
        Query q = new Query("observations", filter);
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
             ds.getFeatureReader(q, Transaction.AUTO_COMMIT)) {
            while (reader.hasNext()) { reader.next(); count++; }
        }
        return count;
    }

    private long countViaJdbc(String whereClause) throws Exception {
        String sql = "SELECT COUNT(*) FROM spatial_iceberg.spatial.observations " + whereClause;
        try (Connection conn = DriverManager.getConnection(
                 "jdbc:trino://localhost:8080/spatial_iceberg/spatial",
                 props());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private List<String> fetchFids(int n) throws Exception {
        List<String> fids = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(
                 "jdbc:trino://localhost:8080/spatial_iceberg/spatial",
                 props());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT __fid__ FROM spatial_iceberg.spatial.observations LIMIT " + n)) {
            while (rs.next()) fids.add(rs.getString(1));
        }
        return fids;
    }

    private static java.util.Properties props() {
        var p = new java.util.Properties();
        p.setProperty("user", "trino");
        return p;
    }
}
