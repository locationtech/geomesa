# GeoMesa Trino/Iceberg Spatial Plugin — Architecture

**Status:** Prototype, implemented and benchmarked. Internal to GeoMesa;
not targeting upstream Apache contribution.

---

## Overview

A Trino plugin (`spatial_iceberg` connector) that augments Trino's
built-in Iceberg connector with GeoMesa-style spatial pruning, plus a
GeoTools DataStore so existing GeoMesa consumers can query the same
warehouse over JDBC and get the optimization automatically.

The connector is intentionally **not** a from-scratch implementation:
`SpatialConnectorFactory` bootstraps Trino's stock Iceberg connector
and wraps the resulting `Connector` with `SpatialConnector`, which
overrides only `getMetadata()` to inject spatial-predicate pushdown.
Splits, page sources, writes, and transactions all delegate to the
stock connector unchanged. The connector's intelligence lives entirely
in `SpatialConnectorMetadata.applyFilter`, which walks the constraint
expression and injects bbox-stat + Z2/XZ2 range domains.

Two catalogs coexist in Trino: the stock `iceberg` and the wrapping
`spatial_iceberg`. They read the same Iceberg REST catalog and the
same Parquet files in MinIO. The stock catalog exists as a baseline
for benchmarking the pruning gains the wrapper adds.

---

## Build environment

| Property                    | Value     |
|-----------------------------|-----------|
| `maven.compiler.source/target` | 17     |
| Minimum build JDK           | 25        |
| `trino.version`             | 481       |
| `iceberg.version`           | 1.11.0 (inherited from the geomesa parent — unified with the rest of the reactor) |
| `jts.version`               | 1.19.0    |

Bytecode targets JDK 17, but **`geomesa-trino-plugin`** must be compiled by a **JDK 25**
`javac`: Trino 481's artifacts (`trino-spi`, `trino-iceberg`, …) are Java 25 bytecode
(class-file major version 69), which an older `javac` cannot read. So the build runs Maven on
JDK 17 (the version the rest of GeoMesa + Scala 2.12 + Arrow build on) and forks a **JDK 25
toolchain** for the plugin module only — `maven-toolchains-plugin` is declared in
`geomesa-trino-plugin/pom.xml`, not on the shared `geomesa-trino` parent. Register the toolchain
once with `build/scripts/update-maven-toolchains.sh`. The other modules (`geomesa-trino-datastore`,
`geomesa-trino-benchmark`) are Java-17-clean — the Trino JDBC driver is Java-11 bytecode — and build
on the JDK-17 Maven JVM with no toolchain, so they can be built independently on JDK 17. All modules
are in the default reactor (no `-Ptrino` profile). The deployed plugin runs inside the
`trinodb/trino:481` image, which bundles its own JDK 25 runtime.

---

## Module layout

```
geomesa-trino/
├── geomesa-trino-plugin/         # Trino plugin: SpatialConnector + applyFilter rewrites (JDK 25)
├── geomesa-trino-datastore/      # GeoTools DataStore over Trino JDBC
├── geomesa-trino-benchmark/      # Trino GeoTools DataStore benchmark runner
└── tools/                        # Python ingest + trino JDBC benchmark + tests
```

`geomesa-trino-plugin` contains the spatial-indexing code — the Z2/XZ2
Iceberg `Transform`s, `GeometryType`, and the `SfcBridge` Scala facade over
GeoMesa's `Z2SFC`/`XZ2SFC` (packages `org.locationtech.geomesa.trino.spatial.*`).
This was formerly a separate `iceberg-spatial` module; it has been absorbed into
the plugin and is shaded into the deployed jar along with JTS and `geomesa-z3`.
`geomesa-trino-datastore` depends on neither the plugin nor that code — it talks
to Trino over JDBC and only needs to know the schema conventions.
`geomesa-trino-benchmark` depends on the datastore.

---

## Spatial table schema

Tables follow a GeoParquet-compatible layout. Names with leading
double-underscores are conventionally hidden by the DataStore's
type mapper.

**Geometry-column discovery (naming convention):** The connector treats
any VARBINARY column `X` as a geometry column **if and only if** at least
one of `__X_bbox__`, `__X_z2__`, `__X_xz2__` exists in the same table.
A table may carry multiple geometry columns; each one's bbox + partition
companions are independent. `applyFilter` routes each ST_* conjunct to the
matching geom column's companions, so
`WHERE ST_Intersects(ST_GeomFromBinary(center), A)
   AND ST_Intersects(ST_GeomFromBinary(ellipse), B)` pushes BOTH
`__center_z2__` and `__ellipse_xz2__` predicates. No catalog config or
per-table metadata property is required for this discovery.

| Column           | Type                                  | Role                                                      |
|------------------|---------------------------------------|------------------------------------------------------------|
| `__fid__`        | VARCHAR (required)                    | feature ID; hidden from `SELECT *` projections             |
| `<X>`            | VARBINARY (WKB on disk and on the wire) | geometry payload (any geom column name); spatial SQL wraps in `ST_GeomFromBinary` |
| `__<X>_bbox__`   | ROW(xmin, ymin, xmax, ymax FLOAT)     | per-row bbox; per-leaf Parquet stats drive file pruning    |
| `__<X>_z2__`     | VARCHAR (partition column, optional)  | 16-char zero-padded lowercase unsigned hex of `Z2SFC.index(centroid) << 2` at 31 bits/axis (62-bit non-negative Long, shifted by 2 so the top hex char carries 4 real bits of hemisphere info); `truncate(N_chars)` string partition for point datasets. Mutually exclusive with `__<X>_xz2__` per geom. |
| `__<X>_xz2__`    | VARCHAR (partition column, optional)  | 16-char zero-padded lowercase unsigned hex of `XZ2SFC(g=12).index(envelope)` — a sequence-code Long. Width ≥ 13 required for spatial discrimination (codes ≤ 2^25 share the leading 8 zero hex chars). Mutually exclusive with `__<X>_z2__` per geom. |
| `__vis__` / `visibilities` | VARCHAR (optional)          | per-row Accumulo-style visibility; enforced when `geomesa.security.*` params are set (see §Row entitlements) |
| `dtg`            | TIMESTAMP WITH TIME ZONE (optional)   | event time; `month(dtg)` / `year(dtg)` partition transforms |

Per-table dataset columns (`sensor_id`, `taxi_id`, etc.) sit alongside
the schema columns above. The full per-dataset breakdown lives in the
README.

---

## SpatialConnector

The Trino plugin is registered via `META-INF/services/io.trino.spi.Plugin`
→ `SpatialIcebergPlugin`. The factory does two things at create time:

1. Bootstraps an internal `IcebergPlugin().getConnectorFactories()` and
   forwards the catalog config (with the geomesa-specific keys stripped
   so airlift Bootstrap doesn't reject them as unknown).
2. Constructs a `GeoMesaColumnCatalog` used by `SpatialConnectorMetadata`
   for per-table geometry-column discovery. The catalog discovers geometry
   columns per table by naming convention: any VARBINARY column `X` is
   treated as a geometry column if at least one of `__X_bbox__`,
   `__X_z2__`, or `__X_xz2__` is present. Results are cached on first
   access (one catalog walk per table, not per query).

`SpatialConnector` then wraps the resulting connector. `getMetadata()`
returns a `SpatialConnectorMetadata` that injects spatial predicate
pushdown (bbox-stat domains + Z2/XZ2 range domains). All other connector
methods — including `getPageSourceProvider()` — delegate to the underlying
Iceberg connector unchanged. There is no per-row scan-time transform; the
plugin's value is entirely in the planning-time pushdown.

---

## SpatialConnectorMetadata.applyFilter

The single point where spatial intelligence enters the query plan.
Trino calls `applyFilter` 2–5 times per query during predicate
pushdown; per-table state (bbox sub-field handles, spatial-partition
column handle + kind + bit resolution) is cached behind
`ConcurrentHashMap` to avoid repeated walks.

The pipeline:

1. **Envelope + geom-column extraction.** Walk the `ConnectorExpression`
   collecting ALL spatial predicates via `findAllSpatialMatches`. Two shapes
   are recognized:
   - Direct spatial-function calls: `st_intersects`, `st_within`, or
     `st_contains` against a literal geometry. Each call produces a
     `SpatialMatch(envelope, functionName, geomName)` — the `geomName` is
     extracted from the row-side argument, which is normally a
     `ST_GeomFromBinary(<var>)` call wrapping a `Variable`; the helper
     unwraps that call to find the column name. A bare `Variable` arg is
     also accepted (used in tests). `st_disjoint` is **deliberately excluded**:
     its result set is the rows that do *not* overlap the envelope, so the
     overlap-only bbox/Z2 domains injected below would prune away exactly the
     files holding the answer. Disjoint predicates pass through to the delegate
     and are evaluated row-by-row.
   - A four-conjunct bbox-struct comparison (the shape emitted by the
     DataStore's `TrinoFilterToSQL` and by hand-written benchmark SQL).
     `tryExtractBboxPatternMatch` reconstructs the envelope from
     `xmax >= envMinX AND xmin <= envMaxX AND ymax >= envMinY AND ymin <= envMaxY`
     and recovers the geom name from the bbox struct's parent variable
     (`__<X>_bbox__` → `X`). Tagged as `functionName = "bbox_pattern"`.

2. **Bbox sub-field domain injection.** Resolve the four leaf
   `IcebergColumnHandle`s of the `__<X>_bbox__` struct for the matched
   geom column. Inject four `REAL`-typed `Domain`s into the constraint's
   `TupleDomain`:
   ```
   xmax >= envMinX       xmin <= envMaxX
   ymax >= envMinY       ymin <= envMaxY
   ```
   Iceberg evaluates these against per-leaf Parquet column statistics
   at scan-planning time, skipping non-intersecting files. Works on
   both `spatial_iceberg` and (when written by hand in SQL) on the
   stock `iceberg` connector.

3. **Spatial-partition pruning, if available (multi-geom routing).**
   `applyFilter` collects every ST_* conjunct in the expression tree (not
   just the first). For each conjunct, `extractGeomColumnName` identifies
   the geometry column name by inspecting the row-side arg of the ST_*
   call: typically a `ST_GeomFromBinary(<var>)` call that gets unwrapped
   to the `<var>` name, with a fallback to bare-`Variable` for tests. Each
   geom column name is then resolved through `GeoMesaColumnCatalog` to its
   own set of companions (`__X_bbox__`, `__X_z2__` or `__X_xz2__`), fully
   independently. AND across different geom columns produces independent
   bbox + partition domains that are AND-combined naturally via
   `TupleDomain.intersect`. OR or any unsupported predicate shape results
   in no partition pushdown for that conjunct (it remains in residual).

   Per geom: inspect the table's columns to decide which spatial-partition
   column is present (cached per table via `GeoMesaColumnCatalog`). The
   resolver prefers `__<X>_xz2__` (non-point datasets) over `__<X>_z2__`
   (point datasets); a given geom column holds one or the other, never
   both. If a spatial-partition column is found, derive the effective
   resolution N from the partition spec's `TruncateTransform.width`
   (`N = 4 × width`, where `width` is the number of hex characters
   retained — each char encodes 4 bits), then build a
   `SortedRangeSet<VARCHAR>` constraint on the hex-encoded column:
   - **Z2 path.** Call `Z2Transform.z2RangesAtReferenceHex(env, partitionBits)`
     and inject the resulting `{startHex, endHex}` pairs as contiguous
     closed `Range.range(...)` entries (16-char lowercase zero-padded hex).
     Z2 leaf cells at the same resolution lie in dense intervals along
     the Z-order curve, so a handful of ranges typically cover the
     envelope.
   - **XZ2 path.** Call `XZ2Transform.xz2RangesAtReferenceHex(env, partitionBits)`,
     which emits a hybrid: level-0 cells of the partition grid as wide
     `Range.range(start, start | ((1L << (64 - partitionBits)) - 1))` —
     so any stored value at an apply-level finer than the partition grid
     is covered — and level-1..xyBits cells as point-equality singletons
     pushed as `Range.equal(...)` for geometries whose apply-level lands
     exactly on a higher-level cell's canonical top-left. The
     `Range.equal` form for singletons is load-bearing: an equivalent
     `Range.range(x, x, closed, closed)` interacts pathologically with
     Iceberg's combined-bbox-sub-field manifest pruning and over-prunes
     correctly-bounded files. See `lessons-learned.md` for the diagnosis.

   Iceberg projects each pushed Range through the truncate-string
   partition transform: an N-char truncation of any value inside a
   Range becomes the partition predicate. Manifest-list
   `partition_summaries` pruning skips whole manifests whose
   partition-value set doesn't intersect; the matching files then
   undergo the same row-level filter at scan time. The pushdown is
   NOT surfaced in EXPLAIN's `constraint on [...]` block (only
   identity-partitioned columns show there) — the observable proof is
   in EXPLAIN ANALYZE's reduced scan-input row count.

   For non-point data on the XZ2 path, the ranges come directly from
   upstream `XZ2SFC(g=12).ranges`, which produces a tight cover of
   sequence-code intervals over the query envelope. The bbox-stat
   pruning injected in step 2 layers on top to further reduce files
   whose per-file bbox stats don't intersect the query envelope.

4. **Delegate, then clean up.** Call the wrapped Iceberg metadata's
   `applyFilter` with the augmented `Constraint`. Strip the injected
   bbox and spatial-partition domains from the returned
   `remainingFilter` — they aren't
   in the scan's projected columns, and leaving them there causes
   `PushPredicateIntoTableScan` to fail with a null column-mapping
   error. Row-level correctness is preserved by the original spatial
   predicate, which remains in `remainingExpression`.

A skip guard at the top prevents re-injection on iterative planning
passes: if any `__<X>_bbox__.xmax` already appears in the constraint
summary's domain map, return immediately.

### What `applyFilter` deliberately does NOT do

It does **not** inject a row-level shortcut for `ST_Intersects` (the
`CASE WHEN bbox-contained THEN TRUE ELSE st_intersects(...) END`
form). The reason is structural: Trino's `ConnectorExpression` SPI
has no `Case` / `If` / `SpecialForm` node, only `Variable`, `Constant`,
`Call` (limited to `StandardFunctions`), and `FieldDereference`. The
OR-equivalent form (`bbox-contained OR st_intersects`) is the only
thing expressible, and it measures as a 3.3× slowdown because
Trino's optimizer distributes OR over AND. See `docs/lessons-learned.md`
§§1–2 for measurement and rationale. The `SpatialMatch` record exists
as scaffolding for a future Trino-aware path that could emit the
shortcut via internal IR.

The CASE WHEN shortcut **is** emitted at the SQL layer by
`geomesa-trino-datastore::TrinoFilterToSQL`, so CQL/GeoTools
consumers get it transparently.

---

## geomesa-trino-datastore

A read-only GeoTools `DataStore` over the Trino JDBC driver. No
build-time coupling to `geomesa-trino-plugin` — just plain JDBC —
but the SQL it emits is shaped to exploit the connector's pruning.

Key classes:

| Class                        | Role                                                                 |
|------------------------------|----------------------------------------------------------------------|
| `TrinoDataStore`             | Connection management; JDBC URL `jdbc:trino://...`                   |
| `TrinoDataStoreFactory`      | GeoTools SPI entry; registered via `META-INF/services/...DataStoreFactorySpi` |
| `TrinoFeatureSource`         | Per-table feature source; bounds via `SELECT MIN/MAX __<X>_bbox__.*` |
| `TrinoFeatureReader`         | Cursor-style iteration; WKB → JTS Geometry                            |
| `TrinoFilterToSQL`           | CQL → SQL with shape-aware rewrites (see §1.3 in `lessons-learned.md`)|
| `TrinoSchemaDiscovery`       | Discovers tables and geometry columns (by naming convention) via JDBC  |
| `TrinoTypeMapper`            | Hides `__fid__`, `__<X>_bbox__`, `__<X>_z2__`/`__<X>_xz2__`, `__vis__` from the GeoTools schema |

The DataStore needs the spatial plugin's column layout (the
`__<X>_bbox__` reads in `TrinoFeatureSource.bounds()` and the SQL
emitted by `TrinoFilterToSQL`). The runtime predicate-rewriting
provided by the plugin is what makes those SQL shapes prune;
without it the DataStore would still work but would scan more files.

### Row entitlements

Tables may carry a per-row Accumulo-style visibility expression in a
`visibilities` (FSDS-compatible) or `__vis__` column; NULL/empty is
unrestricted. When any `geomesa.security.*` param is configured, the
datastore builds an `AuthorizationsProvider` (geomesa-security SPI) and ANDs
`is_visible("<col>", '<auths>')` — a scalar UDF registered by the
spatial plugin, same `accumulo-access` engine as every other GeoMesa store —
into every read, count, and bounds query, so filtering runs in Trino workers
and counts stay correct. A client-side predicate re-checks rehydrated
visibility as defense-in-depth.

**Two enforcement layers:**

1. **Datastore layer** (GeoTools/WFS consumers) — described above; active when
   `geomesa.security.*` params are set on the datastore.
2. **Trino layer** (direct SQL / JDBC / BI consumers) — the spatial plugin's
   `VisibilityAccessControl` (a connector `ConnectorAccessControl`) injects the
   same `is_visible(...)` row filter for every query against the
   `spatial_iceberg` catalog. It maps the Trino session identity to an auth set
   via a pluggable `AuthorizationResolver` (default: a properties file named by
   `geomesa.security.auth-mapping-file`, mapping `user.<n>` / `group.<n>` to
   comma-delimited tokens; unknown identity → no auths → only unrestricted
   rows). The visibility column is detected at analysis time and only observed
   data tables are filtered, so `information_schema` and Iceberg `$`-metadata
   tables are untouched. Opt-in: active only when `geomesa.security.*` is set on
   the catalog. `ConnectorAccessControl` denies by default, so the control
   explicitly allows every other operation (guarded by a coverage tripwire test).

**Layer interaction:** when both layers are active on `spatial_iceberg`, the
datastore's own queries are also seen by the Trino layer (the datastore connects
as a fixed Trino service account). Because both layers AND their `is_visible`
conjuncts together, the datastore's service account must be granted the FULL auth
set in the Trino auth-mapping, making the Trino-layer filter a no-op for it so the
datastore's per-request conjunct (the GeoTools user's auths) stays authoritative.
End-user filtering on the datastore path therefore remains a datastore concern;
the Trino layer governs only direct SQL consumers.

**Boundary:** Trino-layer enforcement covers only the `spatial_iceberg` catalog.
The plain `iceberg` catalog points at the same tables and is NOT wrapped — it is
an unfiltered escape hatch, so deployments must not expose it to untrusted users.
Cross-catalog enforcement would require a cluster-wide `SystemAccessControl`
(deferred).

---

## Z2 partitioning and manifest clustering

`Z2Transform.z2RangesAtReferenceHex` runs a recursive
quadrant subdivision to produce a tight set of `{startHex, endHex}`
pairs over Z2 space that cover the query envelope — typically 4–8
ranges for a geographic bbox. The method takes a `partitionBits`
argument and produces 16-char lowercase hex endpoints in canonical
64-bit reference space.

`XZ2Transform` implements the XZ2SFC containing-cell algorithm:
starting from the finest Z2 cell containing the geometry centroid,
ascend to the nearest ancestor cell whose envelope fully contains the
geometry. Conservative — over-selects, never under-selects.
`XZ2Transform.xz2RangesAtReferenceHex` returns the hybrid pushdown set
for non-point data: level-0 cells of the partition grid as wide ranges
`[start, start | ((1L << (64 - partitionBits)) - 1)]` (covering every
fine-apply stored value within the cell) and higher-level cells as
singletons (start == end) covering coarse-apply geometries whose
stored value lands exactly on a parent cell's canonical top-left.

**Effective partition resolution.** The partition spec stores
`truncate(N_chars)` on the VARCHAR Z2/XZ2 column, where each retained
hex character encodes 4 bits. The connector derives the effective
resolution as `N = 4 × width` from the partition spec's
`TruncateTransform.width` at discovery time. There is no separate
storage property; the partition spec is the single source of truth.

Stored Z2 values are 16-char zero-padded lowercase unsigned hex of
`Z2SFC.index(centroid) << 2`. The shift moves the lat/lon hemisphere
bits (positions 60 and 61 in the unshifted Z2SFC output) into positions
62 and 63 — the top hex char — so `truncate(N)` at any `N ≥ 1` carries
4 useful bits per width. Z2SFC's `MaxMask = 0x7fffffff` reserves bits 62
and 63 of the unshifted output, so the shift wastes nothing. Stored XZ2
values are 16-char unsigned hex of `XZ2SFC(g=12).index(envelope)` with
no shift (sequence codes don't expose top-level quadrants on a
power-of-2 bit boundary; the W ≥ 13 floor stays).

The Java side delegates to `SfcBridge.scala`, which wraps the
package-object types in `geomesa-z3:5.4.0`; the Python writer ports
the same math directly. The partition spec's truncate transform buckets
stored values to the effective N at query time by truncating to the
first `N/4` hex characters; the connector derives matching ranges in
the same hex space (also shifted, for Z2) and Iceberg's truncate-string
predicate projection maps them onto partition-prefix values automatically.
SFC outputs are always non-negative, so the unsigned-hex endpoints are
monotonic in byte-lex order — no sign-flip, no midpoint-split logic
is needed.

`GeoMesaColumnCatalog.DEFAULT_BITS = 12` applies only when the partition
spec is missing or malformed. Production tables always encode N via the
partition spec.

---

## Geometry type representation

Geometry is stored as raw WKB bytes (Iceberg `binary` type) and surfaces
in both the `iceberg` and `spatial_iceberg` catalogs as `VARBINARY`. All
spatial SQL wraps the column at read time:
`ST_Intersects(ST_GeomFromBinary(geom), ...)`. Trino's stock geospatial
functions handle the WKB→Geometry decode, which uses slice-aware
thread-local decoders inside the geospatial plugin — much cheaper than a
per-row decode in connector code.

The `iceberg` and `spatial_iceberg` catalogs expose **identical column
shapes** for a given table. The only difference is what
`spatial_iceberg.SpatialConnectorMetadata.applyFilter` does at planning
time: it walks the spatial predicates and injects bbox-stat + Z2/XZ2 range
domains into the constraint summary. That's the entire value-add — no
type overlay, no synthetic columns, no per-row scan-time transform.

**Why no type overlay?** An earlier version exposed `geom` as Trino's
`Geometry` type (overlay applied in `getTableMetadata` /
`getColumnMetadata`) and materialized it via a wrapping
`SpatialPageSourceProvider` that ran `GeoFunctions.stGeomFromBinary` per
row. To avoid that per-row cost on filter paths the connector also
synthesized a `__<X>_wkb__` virtual column (raw VARBINARY view of the
same Iceberg field) and a set of WKB-input UDFs (`wkb_st_intersects`,
etc.) that read from it. Benchmarks showed the overlay's only effect on
hot-path performance was cosmetic — `SELECT geom` returning a Trino
`Geometry` value instead of `VARBINARY`. Once warm, the explicit
`ST_GeomFromBinary(geom)` wrap (the SQL emitted at the
`TrinoFilterToSQL`/benchmark layer) performed at parity with the overlay,
and the connector lost ~500 LOC and a category of classloader-bridging
failure modes. The overlay was removed.

### Alternative: TypeID.GEOMETRY in Iceberg

A deeper alternative — using Iceberg's native `GEOMETRY` typeId (added
in Iceberg 1.9.0) instead of `binary` — would give first-class
Iceberg-side typing. That path is currently blocked on both ends of the
read/write pipeline: Trino's iceberg connector throws
`UnsupportedOperationException` on `GEOMETRY` columns through at least
version 481 (`TypeConverter` still rejects it, even though `VARIANT` is now
supported), and PyIceberg lacks `GeometryType` through 0.11.1. See
`lessons-learned.md` §7 for the full investigation. Adopting it would
require forking `iceberg-core`, maintaining a custom `iceberg-rest`
image, and re-ingesting existing tables. Not required for any current
functionality; the `binary` + explicit `ST_GeomFromBinary` SQL shape
works on both catalogs unchanged.

Components unaffected by such an upgrade: `Z2Transform`, `XZ2Transform`,
`applyFilter`, all integration tests, the build script, all SQL emitted
by `TrinoFilterToSQL`.

---

## Docker stack

`docker-compose.yml` runs three services:

- `minio` — S3-compatible object store; bucket `s3://warehouse/`
- `iceberg-rest` — `tabulario/iceberg-rest:latest`, sqlite-backed
  catalog at `:8181`. The default upstream URI has a malformed query
  string (`mode=memory` ends up in the filename) and no busy-timeout,
  so `CATALOG_URI` is overridden in compose.
- `trino` — `trinodb/trino:481`. Volume-mounts the host
  `./dist/geomesa-trino-plugin/` directory to
  `/usr/lib/trino/plugin/iceberg-spatial/`. Trino loads each subdirectory
  of `/usr/lib/trino/plugin/` as an independent plugin; the container-side
  name `iceberg-spatial` avoids collision with the built-in `iceberg`
  plugin (the host-side name is arbitrary — it's just the mount source).

`build.sh` compiles `geomesa-trino-plugin` and shades the result into
`dist/geomesa-trino-plugin/geomesa-trino-plugin-*.jar`. The shaded jar bundles
the plugin's spatial-indexing code (formerly `iceberg-spatial`), JTS, and
`geomesa-z3`, excluding `trino-spi`/`trino-main` (provided by the runtime) and
SLF4J/Log4j binders (which would conflict with Trino's own).

Two catalogs are defined under `config/trino/catalog/`:

- `iceberg.properties` — `connector.name=iceberg` (stock)
- `spatial_iceberg.properties` — `connector.name=spatial_iceberg`
  (geometry columns are discovered automatically by naming convention;
  no `geomesa.geometry.columns` property needed)

Both point at the same REST catalog and S3 warehouse.

---

## Testing

Unit tests run via `mvn test`. Integration tests require the docker
stack and are gated behind `-DskipITs=false` (skipped by default).

| Module                       | Tests                                           | Type        |
|------------------------------|-------------------------------------------------|-------------|
| `geomesa-trino-plugin`       | `Z2TransformTest`, `Z2SpatialTest`, `XZ2TransformTest`, `GeometryTypeTest` | unit |
| `geomesa-trino-plugin`       | `GeometryTypeConverterTest`, `SpatialConnectorMetadataTest`      | unit |
| `geomesa-trino-plugin`       | `Z2PruningIT`                                   | integration |
| `geomesa-trino-datastore`    | `TrinoTypeMapperTest`, `TrinoFilterToSQLTest`   | unit |
| `geomesa-trino-datastore`    | `TrinoDataStoreIT`                              | integration |
| `tools/`                       | `test_common.py`, `test_partition_spec.py`, `test_z2_parity.py` | pytest |

The Python `test_z2_parity` corpus (`tools/tests/data/z2_parity_corpus.json`)
is generated by `Z2ParityCorpusGenerator` and consumed by both the
Java unit tests and the Python tests, ensuring the two
implementations agree bit-for-bit.

---

## Related docs

- `README.md` — quick start, the three pruning layers explained for
  users, SQL patterns, per-dataset configuration tables.
- `docs/lessons-learned.md` — empirical findings preserved as
  durable rationale: OR-vs-CASE-WHEN, connector SPI limits, filter
  shape soundness, per-table Z2 resolution, point-from-bbox negative
  result, scale notes.
