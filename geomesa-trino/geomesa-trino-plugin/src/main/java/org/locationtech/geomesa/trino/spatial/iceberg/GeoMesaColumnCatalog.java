/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.RealType;
import org.locationtech.geomesa.trino.spatial.GeometryColumn;
import org.locationtech.geomesa.trino.spatial.SpatialIndexKind;
import org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialConnector;
import org.locationtech.geomesa.trino.spatial.iceberg.connector.SpatialConnectorMetadata;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared per-connector cache of {@link GeometryColumn} descriptors, keyed by
 * {@link SchemaTableName}. Owned by {@link SpatialConnector}; populated lazily
 * by {@link SpatialConnectorMetadata} (which has access to the delegate
 * {@code ConnectorMetadata.getColumnHandles}) and read back by it during
 * {@code applyFilter}.
 *
 * <p>Discovery: a VARBINARY column {@code X} becomes a {@link GeometryColumn}
 * iff at least one of {@code __X_bbox__}, {@code __X_z2__}, {@code __X_xz2__}
 * exists in the same table's column map.
 *
 * <p>Cache lifecycle: lazy population with a {@link #DEFAULT_TTL_NANOS 5-minute}
 * expire-after-write TTL, so companion-column DDL is picked up without a
 * restart. Staleness in either direction is prune-safe. DDL forwarded through
 * the spatial connector invalidates eagerly ({@link #invalidate}/
 * {@link #invalidateSchema}); tables dropped outside it (plain iceberg catalog,
 * REST API, maintenance tools) simply expire. Expiry is safe on both caches:
 * geometry descriptors re-discover on the next {@link #resolve}, and the
 * visibility record is re-written at analysis time (before the access control
 * reads it) on every query, with fail-closed semantics if it is ever absent.
 * The visibility cache uses a {@link #VIS_RETENTION_TTL_MULTIPLE longer}
 * retention so an entry never expires between the analysis-time write and the
 * same query's row-filter read.
 */
public final class GeoMesaColumnCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(GeoMesaColumnCatalog.class);

    // Geometry-column naming convention: a base column X has companions
    // __X_bbox__ / __X_z2__ / __X_xz2__. Companions are themselves __-bracketed.
    private static final String COMPANION_PREFIX = "__";
    private static final String BBOX_SUFFIX = "_bbox__";
    private static final String Z2_SUFFIX   = "_z2__";
    private static final String XZ2_SUFFIX  = "_xz2__";

    // Per-row visibility column
    private static final String VIS_COLUMN = "__vis__";

    /** Default time-to-live for cached descriptors; see {@link #resolve}. */
    static final long DEFAULT_TTL_NANOS = Duration.ofMinutes(5).toNanos();

    /** Visibility entries live this many TTLs. Comfortably above one TTL so an
     *  entry is never reclaimed between the analysis-time write and the same
     *  query's row-filter read; every query re-writes it at analysis time. */
    static final int VIS_RETENTION_TTL_MULTIPLE = 4;

    /** Visibility-column record for a table that has been observed via
     *  {@link #recordVisibilityColumn}: the column name, or empty when the
     *  table was observed to have no visibility column. */
    public record ObservedVisibility(Optional<String> column) {}

    private final Cache<SchemaTableName, Map<String, GeometryColumn>> cache;

    /** Per-table visibility-column record, written at analysis time by
     *  {@code SpatialConnectorMetadata.getColumnHandles} and read by the
     *  Trino-layer {@link org.locationtech.geomesa.trino.security.VisibilityAccessControl}.
     */
    private final Cache<SchemaTableName, ObservedVisibility> visColumns;

    /**
     * Creates a catalog with the default TTL and system clock.
     */
    public GeoMesaColumnCatalog() {
        this(DEFAULT_TTL_NANOS, System::nanoTime);
    }

    GeoMesaColumnCatalog(long ttlNanos, LongSupplier clock) {
        Ticker ticker = new Ticker() {
            @Override
            public long read() {
                return clock.getAsLong();
            }
        };
        this.cache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofNanos(ttlNanos))
            .ticker(ticker)
            .build();
        this.visColumns = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofNanos(VIS_RETENTION_TTL_MULTIPLE * ttlNanos))
            .ticker(ticker)
            .build();
    }

    /** Resolves the geometry-column descriptors for a table; cached per table,
     *  re-discovered after the TTL.
     *
     * @param tableName the schema-qualified table name
     * @param session the connector session
     * @param handle the table handle
     * @param delegate the delegate metadata used to read column handles
     * @return the geometry-column descriptors keyed by base column name
     */
    public Map<String, GeometryColumn> resolve(SchemaTableName tableName,
                                                ConnectorSession session,
                                                ConnectorTableHandle handle,
                                                ConnectorMetadata delegate) {
        Map<String, GeometryColumn> cached = cache.getIfPresent(tableName);
        if (cached != null) {
            return cached;
        }
        // Discover OUTSIDE the cache's per-entry load lock: delegate.getColumnHandles is a
        // potentially slow (metadata/network) Iceberg call. Loading it through Cache.get would
        // hold the entry lock for its duration — serializing concurrent planning and risking
        // re-entrancy if the delegate ever touches this catalog. Concurrent misses may each
        // discover redundantly; that's benign (discovery is idempotent; staleness is prune-safe).
        Map<String, GeometryColumn> geoms = discover(delegate.getColumnHandles(session, handle));
        cache.put(tableName, geoms);
        return geoms;
    }

    /** Records which visibility column (if any) a table exposes, detected from
     *  its column names. Called at analysis time so {@link #visibilityColumn}
     *  is warm when the access control runs.
     *
     * @param tableName the schema-qualified table name
     * @param columnNames the table's column names
     */
    public void recordVisibilityColumn(SchemaTableName tableName, Set<String> columnNames) {
        visColumns.put(tableName, new ObservedVisibility(detectVisibilityColumn(columnNames)));
    }

    /** The recorded visibility observation for a table, or empty if the table
     *  has not been observed (caller should fail closed).
     *
     * @param tableName the schema-qualified table name
     * @return the recorded visibility observation, or empty if not yet observed
     */
    public Optional<ObservedVisibility> visibilityColumn(SchemaTableName tableName) {
        return Optional.ofNullable(visColumns.getIfPresent(tableName));
    }

    /** Drops all cached state for a table. Called when forwarded DDL removes or
     *  renames it, so a later table at the same name re-discovers its geometry
     *  and visibility columns instead of being served stale descriptors.
     *
     * @param tableName the schema-qualified table name
     */
    public void invalidate(SchemaTableName tableName) {
        cache.invalidate(tableName);
        visColumns.invalidate(tableName);
    }

    /** Drops cached state for every table in a schema. Called when forwarded DDL
     *  drops or renames the schema.
     *
     * @param schemaName the schema name
     */
    public void invalidateSchema(String schemaName) {
        cache.asMap().keySet().removeIf(tn -> tn.getSchemaName().equals(schemaName));
        visColumns.asMap().keySet().removeIf(tn -> tn.getSchemaName().equals(schemaName));
    }

    /** {@code __vis__} when present, else empty. */
    static Optional<String> detectVisibilityColumn(Set<String> columnNames) {
        return columnNames.contains(VIS_COLUMN) ? Optional.of(VIS_COLUMN) : Optional.empty();
    }

    /** Pure discovery: given a column map, build the geom-column descriptor map.
     *  Visible for unit testing. */
    static Map<String, GeometryColumn> discover(Map<String, ColumnHandle> cols) {
        Map<String, GeometryColumn> result = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnHandle> e : cols.entrySet()) {
            String name = e.getKey();
            // Skip companion columns themselves — only base names become geoms.
            if (name.startsWith(COMPANION_PREFIX) && name.endsWith(COMPANION_PREFIX)) continue;
            boolean hasBbox = cols.containsKey(companion(name, BBOX_SUFFIX));
            boolean hasZ2   = cols.containsKey(companion(name, Z2_SUFFIX));
            boolean hasXz2  = cols.containsKey(companion(name, XZ2_SUFFIX));
            if (!hasBbox && !hasZ2 && !hasXz2) continue;

            Optional<BboxHandles> bbox = hasBbox
                ? resolveBboxHandles(cols.get(companion(name, BBOX_SUFFIX)))
                : Optional.empty();
            Optional<SpatialPartitionHandle> partition =
                resolvePartitionHandle(name, cols, hasXz2, hasZ2);
            result.put(name, new GeometryColumn(name, bbox, partition));
        }
        return result;
    }

    private static Optional<BboxHandles> resolveBboxHandles(ColumnHandle bboxHandle) {
        if (!(bboxHandle instanceof IcebergColumnHandle bboxBase)) return Optional.empty();
        IcebergColumnHandle xmin = bboxSubFieldHandle(bboxBase, "xmin");
        IcebergColumnHandle ymin = bboxSubFieldHandle(bboxBase, "ymin");
        IcebergColumnHandle xmax = bboxSubFieldHandle(bboxBase, "xmax");
        IcebergColumnHandle ymax = bboxSubFieldHandle(bboxBase, "ymax");
        if (xmin == null || ymin == null || xmax == null || ymax == null) return Optional.empty();
        return Optional.of(new BboxHandles(xmin, ymin, xmax, ymax));
    }

    /** XZ2 wins if both companions exist. Per-geom invariant. */
    private static Optional<SpatialPartitionHandle> resolvePartitionHandle(
            String geomName,
            Map<String, ColumnHandle> cols,
            boolean hasXz2, boolean hasZ2) {
        if (hasXz2 && hasZ2) {
            LOG.debug("Geometry column '" + geomName + "' has both " + companion(geomName, Z2_SUFFIX)
                + " and " + companion(geomName, XZ2_SUFFIX) + "; preferring XZ2.");
        }
        if (hasXz2) {
            if (cols.get(companion(geomName, XZ2_SUFFIX)) instanceof IcebergColumnHandle xz2) {
                return Optional.of(new SpatialPartitionHandle(SpatialIndexKind.XZ2, xz2));
            }
        }
        if (hasZ2) {
            if (cols.get(companion(geomName, Z2_SUFFIX)) instanceof IcebergColumnHandle z2) {
                return Optional.of(new SpatialPartitionHandle(SpatialIndexKind.Z2, z2));
            }
        }
        return Optional.empty();
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /** Builds a companion column name, e.g. {@code companion("geom", "_bbox__")} →
     *  {@code "__geom_bbox__"}. */
    private static String companion(String geomName, String suffix) {
        return COMPANION_PREFIX + geomName + suffix;
    }

    private static IcebergColumnHandle bboxSubFieldHandle(IcebergColumnHandle bboxBase,
                                                          String fieldName) {
        io.trino.plugin.iceberg.ColumnIdentity bboxIdentity = bboxBase.getColumnIdentity();
        if (bboxIdentity == null) return null;
        for (io.trino.plugin.iceberg.ColumnIdentity child : bboxIdentity.getChildren()) {
            if (fieldName.equals(child.getName())) {
                return new IcebergColumnHandle(
                    bboxIdentity, bboxBase.getType(),
                    List.of(child.getId()), RealType.REAL,
                    true, Optional.empty());
            }
        }
        return null;
    }
}
