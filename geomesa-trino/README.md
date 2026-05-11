# GeoMesa Trino

GeoMesa-compatible spatial queries over Iceberg + Parquet on MinIO, served by a custom
Trino connector (`spatial_iceberg`) and a Trino-backed GeoMesa DataStore. Three
pruning layers compose end-to-end:

- **Z2/XZ2 truncate-string partition pruning** at the connector level (skip whole
  manifests via Iceberg's projection of pushed VARCHAR ranges through
  `truncate(N_chars)` on `__geom_z2__` / `__geom_xz2__`).
- **Per-file `__geom_bbox__` column-stat pruning** at planning time (works for both
  the spatial connector and stock Iceberg).
- **Row-level CASE WHEN bbox-contained shortcut** in the SQL emitted by the GeoMesa
  DataStore (skips ST_Intersects/ST_Distance for rows whose bbox is fully inside
  the query envelope).

For axis-aligned rectangular WITHIN queries the row-level test is eliminated entirely
— the bbox-contained predicate is exactly equivalent to ST_Within.

## Design docs

| Topic | File |
|---|---|
| Architecture — modules, plugin model, applyFilter machinery, type model | `docs/design.md` |
| Empirical findings — OR-vs-CASE-WHEN, SPI limits, filter-shape soundness, per-table Z2 resolution, point-from-bbox negative result, scale notes | `docs/lessons-learned.md` |

## Quick Start

```bash
make install-trino     # build plugin JAR + install Python deps (one-time)
make up-trino          # start local stack (MinIO + iceberg-rest + Trino), wait for Trino
make ingest-demo-data  # synthetic CONUS observations + regions (~100k rows)
make bench-trino       # SQL spatial benchmark (spatial_iceberg vs iceberg)
```

See `make help` for all available targets, including per-dataset ingests
(`ingest-tdrive`, `ingest-geolife`, `ingest-ais`) and `bench-geomesa` for the full
CQL → JDBC → SimpleFeature read path.

## Environments

Two compose files describe the two supported topologies. There is intentionally
no generic `docker-compose.yml` — pick one explicitly:

| File | What it runs | When to use |
|---|---|---|
| `docker-compose.local.yml` | MinIO + `iceberg-rest` + Trino, all in Docker | Standalone dev — ingest, query, benchmark everything locally |
| `docker-compose.aws.yml` | Trino only, pointed at the shared AWS `iceberg-rest` + S3 backend | Reading the existing AWS warehouse (e.g. `rawob_harrisnextgen`) |

Pick via the `COMPOSE_FILE` Make variable (defaults to local) or pass `-f`
explicitly to `docker compose`:

```bash
# Local — the default; no extra config needed.
make up-trino
docker compose -f docker-compose.local.yml up -d

# AWS — requires .env with AWS credentials and warehouse location.
cp .env.template .env
$EDITOR .env                                          # fill in AWS keys, region, warehouse path
make up-trino COMPOSE_FILE=docker-compose.aws.yml
docker compose -f docker-compose.aws.yml up -d
```

`.env` is git-ignored. The AWS compose file reads `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_S3_ENDPOINT`, and
`ICEBERG_WAREHOUSE` from it and substitutes them into the Trino catalog
properties at container start. The local compose file uses bundled MinIO
credentials and needs no `.env`.

Make targets that talk to the stack (`up-trino`, `down-trino`,
`destroy-trino`, `logs-trino`, `purge-trino`) all honor `COMPOSE_FILE`. Data
ingests (`ingest-*`) and benchmarks (`bench-*`) talk to `localhost:8080` and
don't care which compose file is up — but `purge-trino` assumes the local
`iceberg-rest` is reachable at `localhost:8181`, so it's local-only in practice.

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Docker + Compose | any recent | run the stack |
| Python | 3.10+ | ingest and query scripts |
| uv | any recent | `make install-trino` (`pip` if unavailable) |
| JDK | 17 **and** 25 | Maven runs on JDK 17; only `geomesa-trino-plugin`'s `javac` is forked to a **JDK 25** toolchain (Trino 481 artifacts are Java 25 bytecode). The datastore and benchmark build on JDK 17. Register both JDKs once with `build/scripts/update-maven-toolchains.sh`. |
| Maven | 3.9+ | build the plugin JAR |

## Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│  Trino 481                                                              │
│                                                                         │
│  catalog: iceberg          catalog: spatial_iceberg                     │
│  (stock Iceberg connector) (SpatialConnector wrapping Iceberg)         │
│                             └── SpatialConnectorMetadata                │
│                                  applyFilter() → bbox + Z2 TupleDomain  │
│                                                                         │
│  CQL clients ──► geomesa-trino-datastore ──► TrinoFilterToSQL           │
│                  (CQL filter ──► bbox-overlap + CASE WHEN shortcut SQL) │
└──────────────┬──────────────────────────────────────┬──────────────────┘
               │                                      │
       ┌───────▼──────────┐                  ┌────────▼─────────────────┐
       │  Iceberg REST    │                  │  MinIO (S3-compatible)   │
       │  catalog :8181   │                  │  s3://warehouse/ Parquet │
       └──────────────────┘                  └──────────────────────────┘
```

Two catalogs are intentional:
- **`iceberg`** — stock connector, no spatial awareness; used as a baseline.
- **`spatial_iceberg`** — wrapper that intercepts `applyFilter()` to inject
  TupleDomain constraints on the `__geom_bbox__` struct sub-fields **and** a
  `SortedRangeSet` over `__geom_z2__` partition values. Iceberg uses both for
  manifest-list and per-file pruning at scan-planning time.

## Row entitlements

Tables may carry a per-row visibility expression in a `visibilities`
(FSDS-compatible) or `__vis__` column; NULL/empty rows are unrestricted. When
any `geomesa.security.*` parameter is configured on the DataStore connection,
the DataStore builds an `AuthorizationsProvider` (geomesa-security SPI) and
ANDs a `is_visible` UDF predicate into every read, count, and bounds
query so that filtering runs inside Trino workers and counts stay correct. A
client-side predicate re-checks rehydrated visibility as defense-in-depth.

| Parameter | Required | Description                                                       |
|---|---|-------------------------------------------------------------------|
| `geomesa.security.auths` | optional | Comma-delimited superset of authorizations to be used for queries |
| `geomesa.security.auths.force-empty` | optional | Don't use implicit authorizations from the underlying Trino user  |
| `geomesa.security.auths.provider` | optional | Explicit `AuthorizationsProvider` instance                        |

Direct Trino SQL / JDBC / BI consumers are filtered separately by the spatial
plugin's connector access control, configured as **catalog** properties on
`spatial_iceberg` (not DataStore params):

| Catalog property | Required | Description |
|---|---|---|
| `geomesa.security.auth-resolver` | optional | `file` (default) or a fully-qualified `AuthorizationResolver` class for an external lookup |
| `geomesa.security.auth-mapping-file` | with `file` | Path to a properties file mapping `user.<n>` / `group.<n>` → comma-delimited auth tokens |

Setting either property opts the catalog into Trino-layer enforcement. Only the
`spatial_iceberg` catalog is protected — do not expose the plain `iceberg`
catalog to untrusted users. See `docs/design.md` §Row entitlements.

## How the three pruning layers compose

### Layer 1: Z2/XZ2 partition pruning (truncate-string manifest pruning)

Tables are `truncate(N_chars)`-partitioned on `__<X>_z2__` (or `__<X>_xz2__`
for non-point datasets), a VARCHAR holding the 16-char zero-padded lowercase
unsigned hex of the upstream GeoMesa SFC index. For Z2, the index is
`Z2SFC.index(lon, lat) << 2` — a left-shift by 2 to rotate the lat/lon
hemisphere bits up into the top hex char (Z2SFC reserves bits 62 and 63,
so the shift wastes nothing). At `N_chars = 1` the 16 possible partition
values map to a 4×4 grid of hemispheric quadrants; CONUS occupies 4 of
them. For XZ2, the index is `XZ2SFC(g=12).index(envelope)` with no shift
(sequence codes don't carry geographic info in their high bits in a way
a fixed shift could exploit). SFC outputs are always non-negative, so
unsigned-hex byte-lex order already matches numeric order — no Calrissian
sign-flip is applied. The partition spec's `TruncateTransform` keeps the
first `N_chars` of the hex string, so the effective resolution is
`N = 4 × N_chars` bits and is read from the partition spec at discovery
time — see the per-dataset configuration table below.

**XZ2 width caveat:** at g=12, every sequence code is ≤ 2^25, so every
stored value shares the leading 8 zero hex chars (`"00000000"`). Spatial
discrimination on the truncate-partitioned column only kicks in at
width ≥ 13; narrower widths bucket every row into a single partition.

When a spatial query arrives at `SpatialConnectorMetadata.applyFilter()`, the
connector extracts the query envelope from the predicate (either an `ST_*`
function call or a 4-pattern bbox-struct comparison emitted by the DataStore)
and expands it into a tight cover of hex ranges via
`Z2Transform.z2RangesAtReferenceHex` (delegates to `Z2SFC.ranges`) or
`XZ2Transform.xz2RangesAtReferenceHex` (delegates to `XZ2SFC(g=12).ranges`).
Because SFC outputs are non-negative, the unsigned-hex endpoints are
monotonic in byte-lex order — a single range per SFC range, no
midpoint-split required.
The cover is pushed as a `SortedRangeSet<VARCHAR>` over the partition column;
Iceberg projects each range through the truncate-string transform to a
partition prefix predicate and skips whole manifests whose
`partition_summaries` don't intersect — **without opening them** — the
metadata-side win that scales with table size. The truncate-string
projection is NOT surfaced in EXPLAIN's `constraint on [...]` block; verify
pruning via EXPLAIN ANALYZE's reduced scan-input row count.

### Layer 2: Per-file `__geom_bbox__` column-stat pruning

Each row carries `__geom_bbox__` (an Iceberg struct of `xmin`, `ymin`, `xmax`,
`ymax` as float32). Iceberg writes per-leaf Parquet statistics for the four
bounds. The connector also pushes four REAL-typed domains (`xmax >= envMinX`,
`xmin <= envMaxX`, `ymax >= envMinY`, `ymin <= envMaxY`) into the Iceberg
delegate's TupleDomain. Both connectors evaluate these against per-file column
stats; files whose bbox can't intersect the query envelope are skipped at
planning time.

At identity-Z2 partitioning, this layer converges with Layer 1 on the same file
set — files in non-overlapping Z2 cells have non-overlapping bbox stats. The
"Δ vs ICE" benchmark column reads ~0% on rectangular queries because both
connectors land at the same file count via different mechanisms.

### Layer 3: Row-level CASE WHEN bbox-contained shortcut

For surviving rows, the GeoMesa DataStore's `TrinoFilterToSQL` emits SQL that
short-circuits the expensive geometry test when the row's bbox is fully inside
the query envelope. The form differs by spatial filter type:

| CQL filter | Emitted SQL pattern | Soundness |
|---|---|---|
| `INTERSECTS(geom, polygon)` | `(bbox-overlap) AND CASE WHEN bbox-contained THEN TRUE ELSE ST_Intersects(geom, polygon) END` | bbox⊆env(polygon) ⇒ ST_Intersects=TRUE (sufficient) |
| `WITHIN(geom, axis-aligned rectangle)` | `(bbox-overlap) AND (bbox-contained)` | bbox⊆rect ⇔ ST_Within=TRUE (**exact equivalence** — no row-level ST_Within) |
| `WITHIN(geom, non-rectangular polygon)` | `(bbox-overlap) AND ST_Within(geom, polygon)` | bbox⊆env(polygon) does NOT imply geom⊆polygon |
| `DWITHIN(geom, ref, d)` | `(outer-bbox) AND CASE WHEN bbox-in-inner-inscribed-rect THEN TRUE ELSE ST_Distance(...) ≤ d END` | inner inscribed rect inside d-circle ⇒ all points within d (sufficient) |
| `BBOX(geom, env)` | `bbox-overlap` directly | exact for axis-aligned bbox queries |

CASE WHEN — not OR. Trino's optimizer distributes OR over AND, causing the
expensive predicate to evaluate up to 4× per row (3.3× wall-clock slowdown
measured). CASE WHEN survives the optimizer intact and short-circuits cleanly.
See `docs/lessons-learned.md` for the soundness proofs and empirical
numbers.

## Per-dataset configuration

Each ingest script sets the effective resolution N per geom column by
configuring the partition spec's `TruncateTransform(width=ceil(N/4))` on the
hex-encoded VARCHAR partition column (see
`tools/common.py::partition_spec_for_geoms`). Each retained hex character
encodes 4 bits, so `N = 4 × N_chars`.

`Z2 bits` is the **total** spatial-index width (per-axis = total/2). Cell size
below is per-axis at the equator: longitude = 360°/2^(bits/2), latitude =
180°/2^(bits/2). The same bit-width convention applies to both Z2 and XZ2.

The `Index` column shows which spatial-partition column each table uses.
Point-only tables use `z2` (Z2 cell index of the geometry's bbox centroid).
Non-point tables use `xz2` (XZ2 cell index — the smallest quadtree cell whose
footprint fully contains the geometry's envelope). The connector auto-detects
which column is present per table and routes the appropriate range generator.
See `docs/design.md` and `docs/lessons-learned.md` §8 for why non-point data
needs XZ2.

| Dataset | Index | Z2 bits | Cell size (lon × lat) | Target file size | Rationale |
|---|---|---|---|---|---|
| observations | z2 | 8 | ~22.5° × 11.25° | (default) | CONUS-wide synthetic; coarse partitioning avoids single-row files at ingest scale |
| regions | xz2 | 8 | ~22.5° × 11.25° | (default) | Synthetic polygons; XZ2 prevents partition-level false negatives that Z2-by-centroid would introduce |
| t-drive | z2 | 20 | ~0.35° × 0.18° | 4 MiB | Beijing-concentrated; high resolution unlocks ST_Intersects shortcut |
| geolife | z2 | 18 | ~0.7° × 0.35° | 4 MiB | Beijing + global travel tail (~340 deg² active) |
| ais | z2 | 16 | ~1.4° × 0.7° | 16 MiB | US East Coast Zone 17 (~125 deg² active) |

Effective resolution is encoded in each table's partition spec
(`truncate(__<X>_z2__, N/4)` on the VARCHAR hex column). The connector
derives `N = 4 × width` from the spec at discovery time; no separate
storage property carries it. The reader-side fallback
`GeoMesaColumnCatalog.DEFAULT_BITS = 12` applies only when the partition
spec is missing or malformed.

## SQL Patterns

CQL consumers via the GeoMesa DataStore get the optimized SQL automatically.
Direct-SQL consumers (e.g., `tools/benchmark.py`) write their own SQL — these
patterns let them adopt the same optimization shapes manually.

```sql
-- BBOX: bbox-overlap on the struct fields. Both connectors prune files via
-- per-leaf Parquet stats on __geom_bbox__; SI also pushes Z2 partition pruning.
SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
WHERE "__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
  AND "__geom_bbox__".ymax >=  37 AND "__geom_bbox__".ymin <=  45;

-- ST_Intersects with row-level shortcut. The leading bbox-overlap conjunct
-- triggers file-level + Z2 pruning; the CASE WHEN short-circuits ST_Intersects
-- (and the WKB decode that precedes it) for rows whose bbox is fully inside.
SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
WHERE ("__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
       AND "__geom_bbox__".ymax >= 37 AND "__geom_bbox__".ymin <= 45)
  AND CASE WHEN "__geom_bbox__".xmin >= -80 AND "__geom_bbox__".xmax <= -70
            AND "__geom_bbox__".ymin >= 37  AND "__geom_bbox__".ymax <= 45
           THEN TRUE
           ELSE ST_Intersects(ST_GeomFromBinary(geom), ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'))
      END;

-- ST_Within(geom, axis-aligned-rectangle): equivalent to bbox-contained, no
-- row-level ST_Within needed. Trino consolidates the two conjuncts into BETWEEN.
SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
WHERE ("__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
       AND "__geom_bbox__".ymax >= 37 AND "__geom_bbox__".ymin <= 45)
  AND ("__geom_bbox__".xmin >= -80 AND "__geom_bbox__".xmax <= -70
       AND "__geom_bbox__".ymin >= 37 AND "__geom_bbox__".ymax <= 45);

-- DWITHIN: outer-bbox-overlap (file pruning) + CASE WHEN inner-inscribed-rect
-- (sufficient for distance ≤ d) ELSE exact spherical distance. The inner
-- rectangle's corners land at distance 0.9 × d from ref; rows whose bbox fits
-- inside it skip ST_Distance entirely.
SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
WHERE ("__geom_bbox__".xmax >= -77.94 AND "__geom_bbox__".xmin <= -76.14
       AND "__geom_bbox__".ymax >= 37.91 AND "__geom_bbox__".ymin <= 39.91)
  AND CASE WHEN "__geom_bbox__".xmin >= -77.66 AND "__geom_bbox__".xmax <= -76.42
            AND "__geom_bbox__".ymin >= 38.18 AND "__geom_bbox__".ymax <= 39.64
           THEN TRUE
           ELSE ST_Distance(
               to_spherical_geography(ST_GeomFromBinary(geom)),
               to_spherical_geography(ST_GeometryFromText('POINT (-77.04 38.91)'))
           ) <= 100000
      END;
```

On **both** catalogs, `geom` is `VARBINARY` (raw WKB) — there is no Geometry-type
overlay — so every spatial function call must wrap it with `ST_GeomFromBinary(geom)`,
as shown above. (Trino 481 removed the implicit `VARBINARY → GEOMETRY` coercion that
earlier releases applied, so the wrap is now mandatory, not just conventional.) The
DataStore and benchmarks emit this shape automatically.

## Schema

Tables live in `iceberg.spatial` / `spatial_iceberg.spatial`,
truncate-partitioned on `__geom_z2__` or `__geom_xz2__` (VARCHAR hex column,
`truncate(N_chars)`), and where applicable on `month(dtg)` or `year(dtg)`.
The Z2 bit resolution is per-table; see the per-dataset configuration
table above.

Common columns across all spatial tables:

| Column | Type | Notes |
|---|---|---|
| `__fid__` | VARCHAR (required) | feature id; hidden from `SELECT *` |
| `geom` | VARBINARY | WKB geometry, identical on both catalogs; wrap with `ST_GeomFromBinary(geom)` for spatial functions |
| `dtg` | TIMESTAMP WITH TIME ZONE | event/observation time (where applicable) |
| `__geom_bbox__` | ROW(xmin, ymin, xmax, ymax FLOAT) | per-row bbox; Parquet column stats drive file-level pruning |
| `__geom_z2__` | VARCHAR (partition column) | 16-char zero-padded lowercase unsigned hex of `Z2SFC.index(centroid) << 2` at 31 bits/axis (shift exposes lat/lon hemisphere bits in the top hex char so `truncate(1)` already discriminates by hemispheric quadrant); `truncate(N_chars)`-partitioned for manifest pruning |

Per-table extras:

| Table | Extra columns |
|---|---|
| `observations` | `sensor_id` VARCHAR, `value` DOUBLE, `active` BOOLEAN |
| `regions` | `category` VARCHAR |
| `tdrive` | `taxi_id` INT |
| `geolife` | `user_id` VARCHAR, `track_id` VARCHAR, `altitude_ft` DOUBLE |
| `ais` | `mmsi` INT, `vessel_name` VARCHAR, `vessel_type` INT, `sog` DOUBLE |

## App Scripts

| Script | Purpose |
|---|---|
| `ingest_synthetic.py` | Create tables + generate and write 100k synthetic CONUS observations + 1k regions |
| `ingest_tdrive.py` | Create table + download and ingest T-Drive Beijing taxi GPS data (~2M rows per zip, 9 zips) |
| `ingest_geolife.py` | Create table + download and ingest GeoLife GPS trajectory data (~25M rows, 182 users) |
| `ingest_ais.py` | Create table + download and ingest Marine Cadastre AIS vessel tracks (US coastal, ~5M rows) |
| `benchmark.py` | SQL spatial benchmark across all ingested datasets (bbox pruning vs baseline). Supports `--datasets <table>...` to filter, `--runs N`, `--warmup N`. |
| `purge_tables.py` | DROP all spatial tables; stack stays up |

> **Small-files warning:** Running an ingest script more than once appends Parquet files.
> After a few runs, manifest scanning slows noticeably. Use `make purge-trino` before
> re-ingesting: `make purge-trino && make ingest-demo-data`, `make purge-trino && make ingest-tdrive`, etc.

> **Skipped public datasets:** GTD (requires form-fill), Twitter/X (requires API key),
> ADS-B Exchange (requires paid subscription) were not added.

## Building

The build runs Maven on **JDK 17**; only `geomesa-trino-plugin`'s `javac` is forked to a
**JDK 25** toolchain (Trino 481 is Java 25 bytecode) — the datastore and benchmark build on
JDK 17. All modules are part of the default reactor — no `-Ptrino` profile needed. Register the
toolchain once (idempotent):

```bash
build/scripts/update-maven-toolchains.sh   # writes the JDK 17 + 25 entries to ~/.m2/toolchains.xml
```

```bash
bash build.sh           # build the geomesa-trino-plugin fat JAR (verifies the toolchain)
mvn test                # run unit tests (no stack required)
mvn -DskipTests package # build without running tests
```

The `geomesa-trino-plugin` fat JAR bundles all dependencies except:
- `trino-spi` — provided by Trino's SPI parent classloader
- `log4j-slf4j-impl` / `log4j-slf4j2-impl` / `logback` — excluded to avoid a circular
  SLF4J↔Log4j bridge that `Log4jLoggerFactory` rejects at class init time
- the unused Iceberg catalog / filesystem backends pulled in by `trino-iceberg` — Snowflake
  (the JDBC driver alone is ~230 MB), Azure, GCS, and Alluxio — pruned via `<exclusions>` since
  the deployment uses only the Glue catalog + S3. This trims the jar from ~360 MB to ~159 MB.
  `iceberg-aws`, `trino-filesystem-s3`, and `trino-hive` (+ Coral/Calcite, which `trino-hive`
  needs at runtime on the Glue path) are kept.

The compiled JAR is excluded from git (see `.gitignore`). Run `bash build.sh` (or
`make install-trino`) before the first `docker compose -f docker-compose.<env>.yml
up` on a fresh clone.

## Service Ports

| Service | Port | URL |
|---|---|---|
| Trino | 8080 | `http://localhost:8080` |
| Iceberg REST catalog | 8181 | `http://localhost:8181/v1/config` |
| MinIO S3 API | 9000 | `http://localhost:9000` |
| MinIO Console | 9001 | `http://localhost:9001` (admin / password) |

## Trino CLI

```bash
docker exec -it trino trino --catalog spatial_iceberg --schema spatial
```

```sql
SHOW TABLES;
SELECT COUNT(*) FROM observations;

-- Check the three pruning layers are active. EXPLAIN shows layers 2 & 3
-- directly; layer 1 (truncate-string partition projection) is NOT surfaced
-- in `constraint on [...]` — use EXPLAIN ANALYZE and confirm
-- `Splits: N` and `Input: M rows` are much smaller than the table's totals.
--   1. EXPLAIN ANALYZE shows reduced scan-input rows/splits — Z2/XZ2 partition pushdown
--   2. `geom_bbox_xmax >= ... AND geom_bbox_xmin <= ...` in filterPredicate — bbox-stat pushdown
--   3. (Optional) `CASE WHEN ... THEN TRUE ELSE st_intersects(...)` for row-level shortcut
EXPLAIN
SELECT COUNT(*) FROM observations
WHERE ("__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
       AND "__geom_bbox__".ymax >= 37 AND "__geom_bbox__".ymin <= 45)
  AND CASE WHEN "__geom_bbox__".xmin >= -80 AND "__geom_bbox__".xmax <= -70
            AND "__geom_bbox__".ymin >= 37  AND "__geom_bbox__".ymax <= 45
           THEN TRUE
           ELSE ST_Intersects(ST_GeomFromBinary(geom), ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'))
      END;

-- Inspect Iceberg metadata tables (use single quotes around the SQL when running
-- through bash to avoid the shell eating $files / $manifests).
SELECT count(*) AS files,
       avg(record_count) AS avg_rows,
       avg(file_size_in_bytes) / 1048576.0 AS avg_mib
FROM iceberg.spatial."observations$files";

SELECT path,
       added_data_files_count + existing_data_files_count AS files,
       partition_summaries[1].lower_bound AS z2_prefix_min,
       partition_summaries[1].upper_bound AS z2_prefix_max
FROM iceberg.spatial."observations$manifests"
ORDER BY partition_summaries[1].lower_bound;
```
