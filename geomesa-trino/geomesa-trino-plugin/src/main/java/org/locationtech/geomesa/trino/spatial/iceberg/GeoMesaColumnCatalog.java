/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.RealType;
import org.locationtech.geomesa.trino.spatial.GeometryColumn;
import org.locationtech.geomesa.trino.spatial.SpatialIndexKind;
import org.locationtech.geomesa.trino.spatial.iceberg.SpatialPartitionHandle;
import org.locationtech.geomesa.trino.spatial.connector.SpatialConnector;
import org.locationtech.geomesa.trino.spatial.connector.SpatialConnectorMetadata;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * <p>Partition-spec inspection: when a Z2/XZ2 companion column has a Truncate
 * transform in the table's partition spec, the effective bit resolution N is
 * derived from the truncate width via {@code N = 4 * width}, where {@code width}
 * is the number of hex characters retained (1..16; each hex char encodes 4 bits).
 * The truncate width is read from the partition-spec JSON as a {@code long}.
 *
 * <p>Fallback: {@link #DEFAULT_BITS} (12) is used when the partition spec is
 * absent, does not contain a Truncate field for the column, or the width is not
 * a positive power of two (e.g. legacy tables written before the truncate-
 * partitioning scheme was adopted).
 *
 * <p>Cache lifecycle: lazy population with a {@link #DEFAULT_TTL_NANOS 5-minute}
 * TTL, so companion-column DDL is picked up without a restart. Staleness in
 * either direction is prune-safe.
 */
public final class GeoMesaColumnCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(GeoMesaColumnCatalog.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Pattern matching a Truncate transform string such as {@code truncate[4503599627370496]}. */
    private static final Pattern TRUNCATE_PATTERN =
        Pattern.compile("truncate\\[(\\d+)]", Pattern.CASE_INSENSITIVE);

    /**
     * Default bit resolution used when the partition spec is absent or does not
     * contain a Truncate transform for the geometry column.  Matches the
     * historical default stored via {@code geomesa.partition.<X>.bits}.
     */
    static final int DEFAULT_BITS = 12;

    /**
     * XZ2 partition widths below 13 hex chars (N &lt; 52 bits) cannot discriminate:
     * XZ2SFC(g=12) sequence codes are ≤ ~25 bits, so every stored value shares the
     * leading zero hex chars and {@code truncate(width<13)} buckets the whole table
     * into a single partition. Below this floor we skip the partition handle entirely
     * (per-file bbox-stat pruning still applies); above it, range pushdown is useful.
     */
    private static final int MIN_XZ2_PRUNING_BITS = 52;

    // Geometry-column naming convention: a base column X has companions
    // __X_bbox__ / __X_z2__ / __X_xz2__. Companions are themselves __-bracketed.
    private static final String COMPANION_PREFIX = "__";
    private static final String BBOX_SUFFIX = "_bbox__";
    private static final String Z2_SUFFIX   = "_z2__";
    private static final String XZ2_SUFFIX  = "_xz2__";

    // Per-row visibility column conventions (see TrinoSchemaDiscovery): the
    // FSDS-compatible "visibilities" is preferred over the companion "__vis__".
    private static final String FSDS_VIS_COLUMN = "visibilities";
    private static final String COMPANION_VIS_COLUMN = "__vis__";

    /** Default time-to-live for cached descriptors; see {@link #resolve}. */
    static final long DEFAULT_TTL_NANOS = java.time.Duration.ofMinutes(5).toNanos();

    private record CachedEntry(Map<String, GeometryColumn> geoms, long bornNanos) {}

    private final ConcurrentHashMap<SchemaTableName, CachedEntry> cache =
        new ConcurrentHashMap<>();
    private final long ttlNanos;
    private final java.util.function.LongSupplier clock;

    /** Per-table visibility-column record, written at analysis time by
     *  {@code SpatialConnectorMetadata.getColumnHandles} and read by the
     *  Trino-layer {@link org.locationtech.geomesa.trino.security.VisibilityAccessControl}. Value is the column name
     *  ({@code visibilities}/{@code __vis__}) or empty when the table has none.
     *  An absent key means "not yet observed" — the access control treats that
     *  as fail-closed. Overwritten on every observation, so it tracks DDL. */
    private final ConcurrentHashMap<SchemaTableName, Optional<String>> visColumns =
        new ConcurrentHashMap<>();

    /**
     * Creates a catalog with the default TTL and system clock.
     */
    public GeoMesaColumnCatalog() {
        this(DEFAULT_TTL_NANOS, System::nanoTime);
    }

    GeoMesaColumnCatalog(long ttlNanos, java.util.function.LongSupplier clock) {
        this.ttlNanos = ttlNanos;
        this.clock = clock;
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
        long now = clock.getAsLong();
        CachedEntry cached = cache.get(tableName);
        if (cached != null && now - cached.bornNanos() < ttlNanos) {
            return cached.geoms();
        }
        // Discover OUTSIDE any map lock: delegate.getColumnHandles is a potentially slow
        // (metadata/network) Iceberg call. Running it inside ConcurrentHashMap.compute would hold
        // the bin lock for its duration — serializing concurrent planning and risking re-entrancy
        // if the delegate ever touches this catalog. Concurrent misses may each discover
        // redundantly; that's benign (discovery is idempotent; staleness is prune-safe).
        CachedEntry entry = new CachedEntry(
            discover(delegate.getColumnHandles(session, handle), partitionSpecJsonOf(handle)), now);
        cache.put(tableName, entry);
        return entry.geoms();
    }

    /** Records which visibility column (if any) a table exposes, detected from
     *  its column names. Called at analysis time so {@link #visibilityColumn}
     *  is warm when the access control runs. {@code visibilities} wins over
     *  {@code __vis__}.
     *
     * @param tableName the schema-qualified table name
     * @param columnNames the table's column names
     */
    public void recordVisibilityColumn(SchemaTableName tableName, Set<String> columnNames) {
        visColumns.put(tableName, detectVisibilityColumn(columnNames));
    }

    /** The recorded visibility column for a table: {@code Optional.of(name)} if
     *  it has one, {@code Optional.empty()} if observed to have none, or
     *  {@code null} if not yet observed (caller should fail closed).
     *
     * @param tableName the schema-qualified table name
     * @return the recorded visibility column, or null if not yet observed
     */
    public Optional<String> visibilityColumn(SchemaTableName tableName) {
        return visColumns.get(tableName);
    }

    /** Drops all cached state for a table. Called when forwarded DDL removes or
     *  renames it, so a later table at the same name re-discovers its geometry
     *  and visibility columns instead of being served stale descriptors.
     *
     * @param tableName the schema-qualified table name
     */
    public void invalidate(SchemaTableName tableName) {
        cache.remove(tableName);
        visColumns.remove(tableName);
    }

    /** Drops cached state for every table in a schema. Called when forwarded DDL
     *  drops or renames the schema.
     *
     * @param schemaName the schema name
     */
    public void invalidateSchema(String schemaName) {
        cache.keySet().removeIf(tn -> tn.getSchemaName().equals(schemaName));
        visColumns.keySet().removeIf(tn -> tn.getSchemaName().equals(schemaName));
    }

    /** {@code visibilities} (preferred) or {@code __vis__}, else empty. */
    static Optional<String> detectVisibilityColumn(Set<String> columnNames) {
        if (columnNames.contains(FSDS_VIS_COLUMN)) return Optional.of(FSDS_VIS_COLUMN);
        if (columnNames.contains(COMPANION_VIS_COLUMN)) return Optional.of(COMPANION_VIS_COLUMN);
        return Optional.empty();
    }

    /** Pure discovery: given a column map and the table's partition-spec JSON
     *  string (may be {@code null}), build the geom-column descriptor map.
     *  Visible for unit testing.
     *
     *  <p>The partition-spec JSON is used only to derive the effective bits N
     *  for Z2/XZ2 partition columns.  When {@code partitionSpecJson} is
     *  {@code null} or the relevant Truncate field is absent, {@link #DEFAULT_BITS}
     *  is used as a fallback. */
    static Map<String, GeometryColumn> discover(Map<String, ColumnHandle> cols,
                                                 String partitionSpecJson) {
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
            Optional<SpatialPartitionHandle> partition = resolvePartitionHandle(
                name, cols, partitionSpecJson, hasXz2, hasZ2);
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
            String partitionSpecJson,
            boolean hasXz2, boolean hasZ2) {
        if (hasXz2 && hasZ2) {
            LOG.debug("Geometry column '" + geomName + "' has both " + companion(geomName, Z2_SUFFIX)
                + " and " + companion(geomName, XZ2_SUFFIX) + "; preferring XZ2.");
        }
        if (hasXz2) {
            if (cols.get(companion(geomName, XZ2_SUFFIX)) instanceof IcebergColumnHandle xz2) {
                int bits = deriveBitsForColumn(xz2, partitionSpecJson);
                // XZ2SFC(g=12) sequence codes are ≤ ~25 bits, so truncate(width) with
                // width < 13 (bits < 52) buckets the whole table into one partition —
                // no spatial discrimination. Skip the partition handle so we don't push
                // useless ranges; per-file bbox-stat pruning still narrows the scan.
                if (bits < MIN_XZ2_PRUNING_BITS) {
                    LOG.debug("XZ2 column " + companion(geomName, XZ2_SUFFIX) + " has partition "
                        + "width=" + (bits / 4) + " (bits=" + bits + "); width < 13 yields no "
                        + "partition pruning under g=12 XZ2SFC — using bbox-stat pruning only.");
                    return Optional.empty();
                }
                return Optional.of(new SpatialPartitionHandle(SpatialIndexKind.XZ2, xz2, bits));
            }
        }
        if (hasZ2) {
            if (cols.get(companion(geomName, Z2_SUFFIX)) instanceof IcebergColumnHandle z2) {
                int bits = deriveBitsForColumn(z2, partitionSpecJson);
                return Optional.of(new SpatialPartitionHandle(SpatialIndexKind.Z2, z2, bits));
            }
        }
        return Optional.empty();
    }

    /** Derive the bit resolution N for a partition column, falling back to
     *  {@link #DEFAULT_BITS} when the spec is absent or has no matching Truncate. */
    private static int deriveBitsForColumn(IcebergColumnHandle col, String partitionSpecJson) {
        OptionalInt bits = deriveBitsFromPartitionSpecJson(partitionSpecJson, col.getId());
        return bits.isPresent() ? bits.getAsInt() : DEFAULT_BITS;
    }

    // -----------------------------------------------------------------------
    // Public helpers (package-private so tests can reach them)
    // -----------------------------------------------------------------------

    /** Derive effective Z2/XZ2 bits N from a truncate-string transform width.
     *  Width is the number of hex chars retained (1..16); each char encodes 4 bits.
     *  So N = 4 * width. */
    static int deriveBitsFromTruncateWidth(long width) {
        if (width <= 0 || width > 16) {
            throw new IllegalArgumentException(
                "TruncateTransform width on a hex-encoded Z2/XZ2 column must be in "
                + "[1, 16] (number of hex chars to retain), got " + width);
        }
        return (int) (4 * width);
    }

    /**
     * Scan the partition-spec JSON for a field whose {@code source-id} matches
     * {@code sourceFieldId}; if the field's transform is a Truncate, return N
     * derived from the truncate width.
     *
     * <p>The transform string {@code "truncate[N]"} is matched via regex and
     * the width is parsed as {@code long}. Under hex-string partitioning the
     * width is the number of hex chars retained (1..16), and N = 4 * width.
     *
     * @param partitionSpecJson  Iceberg partition-spec JSON (may be {@code null})
     * @param sourceFieldId      the Iceberg field ID of the source column
     * @return the derived N, or empty if no matching Truncate field is found
     */
    static OptionalInt deriveBitsFromPartitionSpecJson(String partitionSpecJson, int sourceFieldId) {
        if (partitionSpecJson == null || partitionSpecJson.isBlank()) return OptionalInt.empty();
        try {
            JsonNode spec = MAPPER.readTree(partitionSpecJson);
            JsonNode fields = spec.get("fields");
            if (fields == null || !fields.isArray()) return OptionalInt.empty();
            for (JsonNode field : fields) {
                JsonNode srcId = field.get("source-id");
                if (srcId == null || srcId.asInt() != sourceFieldId) continue;
                JsonNode transformNode = field.get("transform");
                if (transformNode == null) continue;
                Matcher m = TRUNCATE_PATTERN.matcher(transformNode.asText());
                if (!m.matches()) continue;
                long width = Long.parseLong(m.group(1));
                try {
                    return OptionalInt.of(deriveBitsFromTruncateWidth(width));
                } catch (IllegalArgumentException ex) {
                    LOG.debug("Ignoring out-of-range truncate width " + width
                        + " for source field " + sourceFieldId);
                    return OptionalInt.empty();
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to parse partition spec JSON: " + e.getMessage());
        }
        return OptionalInt.empty();
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /** Builds a companion column name, e.g. {@code companion("geom", "_bbox__")} →
     *  {@code "__geom_bbox__"}. */
    private static String companion(String geomName, String suffix) {
        return COMPANION_PREFIX + geomName + suffix;
    }

    /** Extract the current partition-spec JSON string from an IcebergTableHandle.
     *  Trino 481 replaced the single {@code getPartitionSpecJson()} with a per-spec
     *  map ({@code getPartitionSpecJsons()}) keyed by spec id, plus {@code getSpecId()}
     *  for the table's current spec — so we look the active spec's JSON up by id. */
    private static String partitionSpecJsonOf(ConnectorTableHandle h) {
        if (h instanceof io.trino.plugin.iceberg.IcebergTableHandle ith
                && ith.getSpecId().isPresent()) {
            return ith.getPartitionSpecJsons().get(ith.getSpecId().getAsInt());
        }
        return null;
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
