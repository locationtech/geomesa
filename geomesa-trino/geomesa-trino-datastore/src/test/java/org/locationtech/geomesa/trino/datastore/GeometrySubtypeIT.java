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
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end check for GEOMESA-3584: when an Iceberg table carries a GeoMesa-encoded SFT
 * (written by the tooling ingest as the {@code geomesa.sft.spec} table property), schema
 * discovery binds each geometry attribute to the subtype the SFT declares.
 *
 * <p>Requires a running Trino at localhost:8080 with the synthetic demo re-ingested by a build
 * that writes the SFT property (the {@code spatial.regions} table is XZ2-partitioned, so the
 * naming heuristic alone would bind it to generic {@code Geometry} — binding {@code Polygon}
 * proves the SFT drove it). Skips when the tables are absent.
 */
@Tag("integration")
class GeometrySubtypeIT {

    private static DataStore ds;
    private static Set<String> tables = Set.of();

    @BeforeAll
    static void connect() throws Exception {
        try (var socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress("localhost", 8080), 2000);
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Trino not reachable at localhost:8080 — skipping");
        }
        ds = DataStoreFinder.getDataStore(Map.of(
            "trino.host", "localhost", "trino.port", 8080,
            "trino.catalog", "spatial_iceberg", "trino.schema", "spatial"));
        assertThat(ds).as("DataStoreFinder must locate TrinoDataStoreFactory").isNotNull();
        tables = Arrays.stream(ds.getTypeNames()).collect(Collectors.toSet());
    }

    @AfterAll
    static void disconnect() {
        if (ds != null) ds.dispose();
    }

    @Test
    void xz2PolygonColumnBindsToPolygonFromSft() throws Exception {
        Assumptions.assumeTrue(tables.contains("regions"),
            "spatial.regions not ingested (needs an SFT-writing ingest) — skipping");
        SimpleFeatureType sft = ds.getSchema("regions");
        assertThat(sft.getGeometryDescriptor().getType().getBinding())
            .as("XZ2 column bound to its SFT-declared subtype, not generic Geometry")
            .isEqualTo(Polygon.class);
    }

    @Test
    void sftNameRecordedInUserData() throws Exception {
        Assumptions.assumeTrue(tables.contains("regions"),
            "spatial.regions not ingested (needs an SFT-writing ingest) — skipping");
        SimpleFeatureType sft = ds.getSchema("regions");
        assertThat(sft.getUserData().get(TrinoSchemaDiscovery.SFT_NAME_PROPERTY))
            .as("original GeoMesa type name recorded from geomesa.sft.name")
            .isEqualTo("regions");
    }

    @Test
    void z2PointColumnRemainsPoint() throws Exception {
        Assumptions.assumeTrue(tables.contains("observations"),
            "spatial.observations not ingested — skipping");
        SimpleFeatureType sft = ds.getSchema("observations");
        assertThat(sft.getGeometryDescriptor().getType().getBinding()).isEqualTo(Point.class);
    }

    @Test
    void multiGeomBindsEachSubtypeIndependently() throws Exception {
        Assumptions.assumeTrue(tables.contains("observations_2geom"),
            "spatial.observations_2geom not ingested — skipping");
        SimpleFeatureType sft = ds.getSchema("observations_2geom");
        assertThat(sft.getDescriptor("center").getType().getBinding()).isEqualTo(Point.class);
        assertThat(sft.getDescriptor("ellipse").getType().getBinding()).isEqualTo(Polygon.class);
    }
}
