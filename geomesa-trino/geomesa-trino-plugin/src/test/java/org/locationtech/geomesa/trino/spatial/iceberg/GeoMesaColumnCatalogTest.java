/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.GeometryColumn;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class GeoMesaColumnCatalogTest {

    private record FakeColumnHandle(String name) implements ColumnHandle {}

    private static Map<String, ColumnHandle> cols(String... names) {
        Map<String, ColumnHandle> m = new LinkedHashMap<>();
        for (String n : names) m.put(n, new FakeColumnHandle(n));
        return m;
    }

    @Test
    void discoversSingleGeomWithAllCompanions() {
        Map<String, GeometryColumn> result = GeoMesaColumnCatalog.discover(
            cols("__fid__", "geom", "__geom_bbox__", "__geom_z2__"));

        assertThat(result).containsOnlyKeys("geom");
        GeometryColumn g = result.get("geom");
        assertThat(g.name()).isEqualTo("geom");
        // bbox/partition handles only resolve for real IcebergColumnHandles; with
        // FakeColumnHandle they stay empty. This test asserts discovery names only.
        assertThat(g.bbox()).isEmpty();
        assertThat(g.partition()).isEmpty();
    }

    @Test
    void discoversMultipleGeomsIndependently() {
        Map<String, GeometryColumn> result = GeoMesaColumnCatalog.discover(
            cols("__fid__", "center", "ellipse",
                 "__center_bbox__", "__center_z2__",
                 "__ellipse_bbox__", "__ellipse_xz2__"));

        assertThat(result).containsOnlyKeys("center", "ellipse");
    }

    @Test
    void geomWithoutAnyCompanionIsNotADiscoveredGeometry() {
        // Bare varbinary column with no companions → not a geom under naming convention.
        Map<String, GeometryColumn> result = GeoMesaColumnCatalog.discover(
            cols("__fid__", "geom"));
        assertThat(result).isEmpty();
    }

    @Test
    void orphanCompanionWithoutBaseColumnIsIgnored() {
        // __foo_bbox__ with no `foo` column → ignored.
        Map<String, GeometryColumn> result = GeoMesaColumnCatalog.discover(
            cols("__fid__", "geom", "__geom_bbox__", "__foo_bbox__"));
        assertThat(result).containsOnlyKeys("geom");
    }

    @Test
    void bboxOnlyGeomHasNoPartition() {
        Map<String, GeometryColumn> result = GeoMesaColumnCatalog.discover(
            cols("__fid__", "geom", "__geom_bbox__"));
        assertThat(result).containsOnlyKeys("geom");
    }

    @Test
    void partitionOnlyGeomIsDiscovered() {
        Map<String, GeometryColumn> result = GeoMesaColumnCatalog.discover(
            cols("__fid__", "geom", "__geom_z2__"));
        assertThat(result).containsOnlyKeys("geom");
    }

    @Test
    void unrelatedVarbinaryColumnsDoNotBecomeGeoms() {
        Map<String, GeometryColumn> result = GeoMesaColumnCatalog.discover(
            cols("__fid__", "geom", "__geom_bbox__", "other_blob"));
        assertThat(result).containsOnlyKeys("geom");
    }

    // -----------------------------------------------------------------------
    // Cache TTL
    // -----------------------------------------------------------------------

    /** Swappable column map simulating ALTER TABLE; counts discovery calls. */
    private static final class MutableDelegate implements ConnectorMetadata {
        Map<String, ColumnHandle> columns;
        int calls;

        MutableDelegate(Map<String, ColumnHandle> columns) { this.columns = columns; }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                          ConnectorTableHandle tableHandle) {
            calls++;
            return columns;
        }
    }

    @Test
    void cacheEntriesExpireAfterTtlSoSchemaChangesAreEventuallyPickedUp() {
        AtomicLong clock = new AtomicLong(0);
        GeoMesaColumnCatalog catalog =
            new GeoMesaColumnCatalog(Duration.ofMinutes(5).toNanos(), clock::get);
        SchemaTableName tn = new SchemaTableName("s", "t");
        MutableDelegate delegate = new MutableDelegate(cols("__fid__", "geom"));

        assertThat(catalog.resolve(tn, null, null, delegate)).isEmpty();

        delegate.columns = cols("__fid__", "geom", "__geom_bbox__", "__geom_z2__");
        clock.addAndGet(Duration.ofMinutes(5).toNanos() + 1);
        assertThat(catalog.resolve(tn, null, null, delegate)).containsOnlyKeys("geom");
    }

    @Test
    void cacheEntriesAreReusedWithinTtl() {
        AtomicLong clock = new AtomicLong(0);
        GeoMesaColumnCatalog catalog =
            new GeoMesaColumnCatalog(Duration.ofMinutes(5).toNanos(), clock::get);
        SchemaTableName tn = new SchemaTableName("s", "t");
        MutableDelegate delegate = new MutableDelegate(cols("__fid__", "geom", "__geom_bbox__"));

        catalog.resolve(tn, null, null, delegate);
        clock.addAndGet(Duration.ofMinutes(4).toNanos());
        catalog.resolve(tn, null, null, delegate);

        assertThat(delegate.calls).isEqualTo(1);
    }

    // -----------------------------------------------------------------------
    // Visibility-entry expiry: reclaiming entries for tables dropped outside
    // forwarded DDL
    // -----------------------------------------------------------------------

    @Test
    void visibilityEntriesExpireForTablesDroppedOutsideForwardedDdl() {
        long ttl = Duration.ofMinutes(5).toNanos();
        AtomicLong clock = new AtomicLong(0);
        GeoMesaColumnCatalog catalog = new GeoMesaColumnCatalog(ttl, clock::get);
        SchemaTableName dropped = new SchemaTableName("s", "dropped_externally");
        catalog.recordVisibilityColumn(dropped, java.util.Set.of("geom", "__vis__"));
        assertThat(catalog.visibilityColumn(dropped).orElseThrow().column()).contains("__vis__");

        // Past the retention window with no refresh (the table was dropped via the
        // plain iceberg catalog, so no invalidate() ran).
        clock.addAndGet(GeoMesaColumnCatalog.VIS_RETENTION_TTL_MULTIPLE * ttl + 1);

        // Empty = "not observed" — a recreated table re-records at analysis time,
        // and until then the access control fails closed.
        assertThat(catalog.visibilityColumn(dropped)).isEmpty();
    }

    @Test
    void activelyRefreshedVisibilityEntriesDoNotExpire() {
        long ttl = Duration.ofMinutes(5).toNanos();
        AtomicLong clock = new AtomicLong(0);
        GeoMesaColumnCatalog catalog = new GeoMesaColumnCatalog(ttl, clock::get);
        SchemaTableName live = new SchemaTableName("s", "live");
        catalog.recordVisibilityColumn(live, java.util.Set.of("geom", "__vis__"));

        // Refreshed at 2×TTL (every analyzed query re-records), read at 5×TTL:
        // age since refresh is 3×TTL, inside the retention window.
        clock.addAndGet(2 * ttl);
        catalog.recordVisibilityColumn(live, java.util.Set.of("geom", "__vis__"));
        clock.addAndGet(3 * ttl);

        assertThat(catalog.visibilityColumn(live).orElseThrow().column()).contains("__vis__");
    }
}
