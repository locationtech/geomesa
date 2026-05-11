# Lessons Learned

Empirical findings and design rationale captured during the prototype.
These are deliberately preserved because each one is a load-bearing
constraint on the current code, or a plausible-seeming optimization
that didn't survive measurement and would otherwise be re-attempted.

---

## 1. CASE WHEN beats OR for the row-level spatial shortcut

`tools/benchmark.py` rewrites `ST_Intersects(geom, env)` into the form:

```sql
CASE WHEN bbox-contained-in-envelope THEN TRUE
     ELSE ST_Intersects(geom, env)
END
```

The earlier OR-equivalent form, `bbox-contained OR ST_Intersects(geom, env)`,
**must not** be used. Measurement on the geolife dataset:

| Form                 | SI wall-clock (5-run mean) |
|----------------------|----------------------------|
| Baseline (no rewrite) | 8,476 ms                  |
| OR form               | 29,777 ms (**3.3× slowdown**) |
| CASE WHEN form        | 4,927 ms (**1.72× speedup**) |

Why OR is fatal: Trino's optimizer distributes OR over AND, so
`(p_xmin AND p_xmax AND p_ymin AND p_ymax) OR st_intersects(...)` becomes
four disjuncts each containing `st_intersects(...)`. The spatial
predicate gets evaluated up to 4× per row. CASE WHEN survives the
optimizer intact and short-circuits the ELSE branch.

Soundness: `bbox ⊆ envelope ⇒ ST_Intersects = TRUE`. This is a
one-way implication and only valid for `ST_Intersects`. `ST_Within`,
`ST_Contains`, and `ST_Disjoint` cannot use the same shortcut.

---

## 2. Connector-level CASE WHEN injection is structurally impossible

Trino's `ConnectorExpression` API (the SPI a connector uses to rewrite
predicates in `applyFilter`) has **no CASE / IF / SpecialForm node**.
Available expressions are limited to `Variable`, `Constant`, `Call`,
and `FieldDereference`. `Call` only accepts function names recognized
by `StandardFunctions` — `$and`, `$or`, `$not`, comparisons, arithmetic,
`$nullif`, `$like`, `$in`. There is no `$case` or `$if`.

The CASE WHEN that works at the SQL layer lives in Trino's internal IR
(`io.trino.sql.ir.Case`), constructed by the parser *before* the
connector ever sees the predicate.

This is why `SpatialConnectorMetadata` does not inject a row-level
shortcut: the OR equivalent is the only form expressible via
`ConnectorExpression`, and OR is fatal (see §1). The production path
uses `findAllSpatialMatches` (multi-match collector) for partition +
bbox pushdown only; `findSpatialMatch` (single-match convenience) is
retained for test fixtures and would be the entry point for any
future Trino-aware row-level shortcut.

**Where the shortcut does live:** `geomesa-trino-datastore`'s
`TrinoFilterToSQL` emits CASE WHEN directly into the SQL it generates,
so CQL/GeoTools consumers get the optimization automatically. Direct-SQL
callers (the benchmark) write CASE WHEN by hand.

---

## 3. Three filter shapes, three soundness regimes

`TrinoFilterToSQL` emits different SQL depending on the CQL filter
shape, because the soundness of each rewrite depends on the geometric
relationship between the row bbox, its envelope, and the query.

| CQL                            | Emitted SQL                                                                                                                                                | Soundness                                                              |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| `INTERSECTS(geom, polygon)`    | `(bbox-overlap) AND CASE WHEN bbox-contained THEN TRUE ELSE ST_Intersects(geom, polygon) END`                                                              | `bbox ⊆ env(polygon) ⇒ ST_Intersects = TRUE` (sufficient only)        |
| `WITHIN(geom, axis-aligned rectangle)` | `(bbox-overlap) AND (bbox-contained)`                                                                                                              | `bbox ⊆ rect ⇔ ST_Within = TRUE` (**exact equivalence**)              |
| `WITHIN(geom, non-rectangular polygon)` | `(bbox-overlap) AND ST_Within(geom, polygon)`                                                                                                     | `bbox ⊆ env(polygon)` does **not** prove `geom ⊆ polygon`             |
| `DWITHIN(geom, ref, d)`        | `(outer-bbox-overlap) AND CASE WHEN bbox-in-inner-inscribed-rect THEN TRUE ELSE ST_Distance(...) ≤ d END`                                                  | `bbox ⊆ inner-inscribed-rect ⇒ all-points-within-d` (sufficient only) |

The `WITHIN`-rectangle case is uniquely strong: because a geometry's
bbox is its tight bounding rectangle, and an axis-aligned rectangle
equals its own envelope, both directions of the implication hold and
no geometry decode is needed. Trino consolidates the four bbox
predicates into `BETWEEN` expressions automatically. Performance
approaches BBOX-only queries.

The shape detection is `JTS Polygon.isRectangle()` — a 5-point ring
whose four corners form an axis-aligned rectangle.

---

## 4. Z2/XZ2 resolution: per-table, encoded in the partition spec

The spatial partition is `truncate(__<X>_z2__, N_chars)` (or `__<X>_xz2__`)
on a VARCHAR column holding 16-char zero-padded lowercase unsigned hex.

**Z2 encoding: `Z2SFC.index(centroid) << 2`.** Z2SFC at 31 bits/axis
emits a 62-bit non-negative Long, leaving bits 62 and 63 unused (the
`MaxMask = 0x7fffffff` reserves them). We left-shift by 2 before hex
formatting to rotate the lat/lon hemisphere bits (positions 60 and 61
in the unshifted value) up to positions 62 and 63 — the top hex char.
This lets `truncate(N)` discriminate by 4 useful bits per width at any
`N ≥ 1`. At `N = 1` the 16 partition values map to a 4×4 grid of
hemispheric quadrants (lat halved twice × lon halved twice); CONUS
occupies 4 of them (prefixes `8, 9, a, b`).

Range generation applies the same shift: each `[lo, hi]` from
`Z2SFC.ranges` becomes `[lo << 2, (hi << 2) | 3]`. The OR-3 is
defensive; stored shifted values always have low 2 bits = 0, so
`hi << 2` would suffice.

**XZ2 encoding: `XZ2SFC(g=12).index(envelope)` with no shift.** Sequence
codes at g=12 are ≤ 2^25, so every stored hex value shares the leading
8 zero hex chars. The 4 top-level quadtree quadrants land at sequence-code
offsets `1, ~5.6M, ~11.2M, ~16.8M` — not aligned to a power-of-2 bit
boundary, so a fixed shift can't expose them in a clean prefix. Truncate
widths below 13 therefore produce a single bucket for every row in the
table. At column-discovery time the connector detects this case (bits < 52)
and skips the XZ2 partition handle entirely — it pushes no partition ranges
(they would match every partition anyway), and per-file `__<X>_bbox__`-stat
pruning carries the scan reduction. The synthetic `regions` and
`observations_2geom.ellipse` columns are at width 2 and exercise exactly this
bbox-stat-only path.

**Why hex strings, not BIGINTs?** Iceberg's `Truncate.fromString` parses
the width via `Integer.parseInt`, capping `truncate(long, W)` at
`W ≤ Integer.MAX_VALUE ≈ 2³¹`. To partition at a resolution finer than
`64 − 31 = 33` bits, the truncate width must exceed `Integer.MAX_VALUE`,
which Iceberg-Java cannot encode. The hex-string representation
sidesteps the cap entirely: each char encodes 4 bits, so
`truncate(string, 1..16)` covers the full 4..64-bit resolution range
without ever crossing the int width limit.

**No Calrissian sign-flip.** SFC outputs are non-negative, so unsigned
hex already preserves byte-lex order. The sign-flip would add a constant
1 to the top bit, shifting partition prefixes from {0–f} to {8–f} for
shifted Z2 — no discrimination change, just cosmetic.

**Why delegate to upstream `geomesa-z3`?** The prior homegrown
`spreadBits` / canonical-32-bit-grid logic produced its own Z2 / XZ2
encoding that diverged from the cloud writer. We now wrap
`org.locationtech.geomesa.curve.{Z2SFC, XZ2SFC}` via `SfcBridge.scala`
in `iceberg-spatial` (Scala because Java cannot reference the
package-object types `package$IndexRange` / `package$ZRange` — JLS
keyword conflict). The Python writer ports the same math directly
(Python can't call Scala). Parity is verified bit-for-bit against the
shared corpus regenerated by `Z2ParityCorpusGenerator`.

Both Java `Z2Transform.hexEncode` and Python `_hex_encode` produce
identical 16-char unsigned hex; verified by `tools/tests/test_z2_parity.py`
against the shared corpus.

The connector's range generators (`z2RangesAtReferenceHex`,
`xz2RangesAtReferenceHex`) delegate to `Z2SFC.ranges` /
`XZ2SFC(g=12).ranges` and hex-encode each endpoint. Because SFC outputs
are always non-negative, the unsigned-hex endpoints are monotonic in
byte-lex order — a single hex range per SFC range, no midpoint-split
logic required. Iceberg's truncate-string predicate projection maps
each pushed Range onto partition-prefix values automatically.

The `geomesa.partition.<X>.bits` storage property has been retired. The
partition spec's `TruncateTransform.width` IS the canonical source of N
(recoverable as `N = 4 × width`).

At prototype scale most partition cells contain 0–1 rows and Parquet
files are small. Pruning still works correctly; file sizes are just
far from optimal.

---

## 5. Negative result: reconstructing Point from `__geom_bbox__`

A plausible optimization: detect point-only tables at runtime, then
project `__geom_bbox__.xmin` / `__geom_bbox__.ymin` from Parquet
instead of `geom`, and reconstruct `Point(lon, lat)` client-side.
This would eliminate per-row WKB decode for the most common geometry
type.

**It was implemented, measured, and reverted.** Empirical results:

| Dataset  | Rows | Files | Rows/file | SI delta vs baseline |
|----------|------|-------|-----------|----------------------|
| t-drive  | 2.1M | 205   | ~10k      | **1.20× faster** ✓  |
| geolife  | 25M  | 4,655 | ~5k       | **~1.07× slower** ✗ |

It helped on small/dense files and hurt on large/sparse files — and
geolife was the dataset that most needed it. Reverted in commits
`1df5d6b8f6 … 35bc93c62a`.

**Probable root cause:** the optimization changes Parquet's column
read pattern from **1 column chunk per file** (`geom` as one binary
column) to **2 column chunks** (`__geom_bbox__.xmin` and `.ymin` are
separate Parquet columns since the struct is leaf-flattened). Per-chunk
overhead (file open, dictionary read, page header parse) doubles. For
datasets with high file count and low rows-per-file, the extra
per-chunk overhead exceeds the WKB-decode savings.

**Why we reverted rather than gating behind a flag:** a flag users
must know to set means most won't get the win where it helps and
won't avoid the loss where it hurts. The asymmetric-risk pattern
("sometimes a loss") is a worse default than "no change."

**If you revisit:** profile the geolife scan to confirm the
column-fragmentation hypothesis before reattempting. The fix is
probably storage-layer (GeoParquet "covering" encoding stores point
coordinates inline with the geom column), not connector-layer.

---

## 6. Scale notes (~1M files)

What the current optimizations contribute at lakehouse scale:

| Layer                                        | Scale behavior                                                       |
|----------------------------------------------|-----------------------------------------------------------------------|
| Z2 manifest-list pruning (SI exclusive)      | **Compounds positively** — bigger benefit at higher manifest counts  |
| File-stat pruning on `__geom_bbox__` (both connectors) | Linear in surviving file count; converges with Z2 at identity-Z2 partitioning |
| Row-level CASE WHEN (INTERSECTS, DWITHIN)    | CPU-only win; modest at I/O-bound scale (1.1–1.3×)                  |
| WITHIN-rectangle (pure metadata)             | Best — performance approaches BBOX                                   |

The row-level shortcut is **CPU-only**: Trino's Parquet reader fetches
column chunks based on the predicate's column references, not the CASE
branch outcome. The geom column chunks get downloaded whether or not
most rows short-circuit. Expect bigger relative wins on small
CPU-bound queries, smaller on I/O-bound queries at scale.

Unrelated lakehouse hygiene the prototype does not address:
- `write.target-file-size-bytes`: demo defaults are 1–4 MiB;
  production wants 64–256 MiB.
- Identity-Z2 partitioning at high bits + globally-distributed data
  can produce millions of partitions; coarser bits or `truncate()`
  bucketing would help.

---

## 7. Implementing the Geometry type overlay — what each spike revealed

Surfacing `geom` as Trino's `Geometry` type instead of `VARBINARY` took four
spikes before a working approach landed. Each one ruled out a plausible
mechanism; recording them prevents re-treading.

### Spike 1: `applyProjection` to rewrite column references

Hypothesis: return `Call(Geometry, "ST_GeomFromBinary", [Variable(geom_varbinary)])`
as the projection for a Geometry-typed `geom` reference. Result: the
planner accepts the result but silently drops the rewrite — `EXPLAIN`
shows the original `Variable(geom)` against the Geometry type, the
synthetic `Call` is absent. `applyProjection` works for pushing complex
projections *down* to the source (struct dereferences, JDBC function
pushdown), not for synthesizing computed values *up* over plain column
reads.

### Spike 2: Wrapping `ColumnHandle` returned from `getColumnHandles`

Hypothesis: return a wrapper `GeometryColumnHandle(underlying, geometryType)`
from `getColumnHandles`; the planner reads the type from the handle.
Result: Trino's iceberg internals do `(IcebergColumnHandle) handle` casts
at multiple points downstream of the metadata layer. Our wrapper isn't
an `IcebergColumnHandle`; the casts fail.

The actual planner reads types from `getTableMetadata`, not from
`getColumnHandles`. Once `getTableMetadata` + `getColumnMetadata` are
overridden to swap VARBINARY → Geometry on annotated columns, type
resolution works without wrapping handles.

### Spike 3: Page-source wrap, but using our shaded `JtsGeometrySerde`

Hypothesis: wrap the page source provider, convert WKB → Geometry slices
via the shaded `io.trino.plugin.geospatial.GeoFunctions.stGeomFromBinary`
bundled in our plugin's fat jar. Result: `ST_AsText`, `ST_GeometryType`,
`ST_AsBinary` work correctly on our slices, but `ST_Intersects` fails
with "Range [21, 21 + 8) out of bounds for length 21". The slice we
produce is 21 bytes — the *raw WKB*, not a Trino-Geometry slice (which
for a Point is 17 bytes: 1-byte type code 0 + 16 bytes of two doubles).
Two distinct serdes (`JtsGeometrySerde` and `GeometrySerde`/Esri) coexist
in Trino's geospatial plugin; functions that use the Esri envelope
fast-path require a specific byte layout that we weren't producing.

### Spike 4 (the working one): reflection through the canonical type's classloader

Hypothesis: call `GeoFunctions.stGeomFromBinary` reflectively, resolved
through the *canonical* `Geometry` type's classloader (i.e. Trino's
geospatial plugin classloader, not our shaded copy). Result: works for
all tested spatial functions, slice format is correct (17 bytes for
POINT), counts match the baseline `iceberg` catalog with explicit
`ST_GeomFromBinary` wrapping.

The Method reference is resolved once at provider construction via
`Class.forName("io.trino.plugin.geospatial.GeoFunctions", true,
geometryType.getClass().getClassLoader())`. Plugin classloader isolation
makes shaded copies *visible but format-incompatible* — the canonical
copy is what every other plugin loads, and what every other spatial
function expects.

### The other load-bearing detail: factory-path wiring

Independent of the serde discovery, `SpatialConnector.getPageSourceProvider()`
alone is insufficient — Trino uses `getPageSourceProviderFactory()` at
execution time. Both methods must be overridden to install the wrapper.
Spike 3's initial failure mode (provider not invoked, no diagnostic
output) was caused by leaving the factory as a pass-through. Once both
methods wrap, the wrap fires for every query.

### Ruled out by spike sequence

- Iceberg's native `GEOMETRY` typeId (added in Iceberg 1.9.0): Trino's
  iceberg connector doesn't support it through Trino 480 (`TypeConverter`
  throws `UnsupportedOperationException`), and PyIceberg lacks the type
  entirely through 0.11.1. Both ends of the read/write pipeline need
  work before this path is viable.
- Custom Trino `Type` with WKB-as-internal-format: would require
  re-registering every spatial function against the custom type. Loses
  the ecosystem benefit.
- Iceberg-level catalog wrapping to rewrite schemas before Trino's
  `TypeConverter` sees them: doable in principle but requires plugging
  into Trino's Guice-bound `TrinoCatalog` factory machinery. Significantly
  more invasive than the metadata-layer overlay.

## 8. XZ2 for non-point datasets: lossy encoding, bbox-stat backstop

The Z2-by-centroid partition column has a correctness gap for non-point
data: a polygon whose centroid sits in a Z2 cell outside the query
envelope can still have its bbox extend into the envelope, and a
partition predicate on `__geom_z2__ ∈ ranges(env)` will exclude it.
False negative.

XZ2 fixes this by storing the smallest ancestor cell whose footprint
fully contains the geometry. A query then enumerates every cell at every
level whose footprint touches the envelope and asserts the column is in
that set. By construction, any geometry whose envelope is inside the
query envelope is in the set — no false negatives.

### Upstream XZ2SFC at g=12 (sequence-code encoding)

`XZ2Transform.apply` now delegates to
`SfcBridge.xz2Index(envelope, g=12)`, which calls upstream
`org.locationtech.geomesa.curve.XZ2SFC(g=12).index`. The result is a
quadtree sequence code — a non-negative Long in roughly `[0, 22M]`. The
cloud GeoMesa writer emits the same bytes; cross-engine table swaps are
byte-exact.

### The query-side range generator

`XZ2Transform.xz2RangesAtReferenceHex(env, partitionBits)` delegates to
`SfcBridge.xz2RangesAsLongs(env, g=12)` (which calls `XZ2SFC.ranges`)
and hex-encodes each endpoint with `Z2Transform.hexEncode`. The
upstream XZ2SFC range generator produces a tight cover of sequence-code
intervals — no hybrid level-0/level-1+ split is needed because each
geometry's stored value lies inside one of those intervals by
construction.

History: an earlier homegrown range generator emitted level-0 wide
ranges plus level-1+ singletons (paired with a homegrown apply that
encoded cells via `spreadBits` on a canonical 32-bits-per-axis grid).
That scheme was specific to the old bit-interleave encoding; the
upstream sequence-code encoding doesn't need it.

### The mutual-exclusion invariant

Tables hold either `__geom_z2__` (point datasets) or `__geom_xz2__`
(non-point datasets), never both. The column name is the type signal;
no auxiliary table property is needed for routing. The connector's
resolver checks for `__geom_xz2__` first and falls back to
`__geom_z2__`. If both ever appear in the same table (accidental
schema), XZ2 wins because its cell-set is correct for both geometry
types.

### Ruled out by design

- Level-tagged encoding (encode level into high bits of the Long).
  Would make partition pruning exact, but the bbox-stat backstop is
  cheap enough and the lossy encoding's over-selection is bounded.
  Defer until benchmarks on a real non-point dataset show the partition
  layer is materially under-pruning.
- Storing both `__geom_z2__` and `__geom_xz2__` per row. Doubles the
  per-row storage cost for the spatial-index column; the only benefit
  is "faster pruning for axis-aligned-rectangle WITHIN queries on
  non-point data" — a niche case the bbox-overlap predicate already
  handles via `__geom_bbox__`.
- A unified `__geom_spatial__` column with a per-table
  `geomesa.spatial.kind` property. Adds a metadata round-trip on every
  query; loses column-name-as-type-signal. The current mutual-exclusion-
  by-naming approach is clearer to humans reading the schema.

### Known follow-ups

- **PyIceberg `IdentityTransform` doesn't validate the source column.**
  Nothing in the Iceberg schema prevents accidentally writing
  `geom_z2_partition` output into a column named `__geom_xz2__` (or vice
  versa). The ingest code is the single source of truth; the Java↔
  Python parity tests catch drift on the helper-function side, but a
  developer can still wire the wrong helper to the wrong column.
  Catching that would require either a runtime sanity check in
  `geom_xz2_partition` (e.g., assert the resulting cell contains the
  geometry) or a value-check at scan time. Out of scope for the
  current prototype.

---

## § 9 — Multi-geometry-column support

Tables can declare any number of geometry columns by following the naming
convention `X` + `__X_bbox__` + `__X_{z2,xz2}__`. Discovery is pure naming
convention; no catalog config or per-table metadata is required. The
connector's `GeometryColumnCatalog` (owned by `SpatialConnector`, populated
and read by `SpatialConnectorMetadata`) caches the per-table descriptor map on
first access.

**Per-geom partition pushdown:** `applyFilter` collects every ST_* conjunct
in the expression tree (not just the first), extracts the geom-column name
from each one's variable arg, and looks up that geom's bbox + partition
companions independently. AND across geoms → independent bbox + partition
domains, AND-combined naturally by `TupleDomain.intersect`. OR or any
unsupported predicate shape → no partition pushdown (the predicate stays
in residual).

**Geom-column extraction shape:** Production SQL emits
`ST_Intersects(ST_GeomFromBinary(geom), <literal>)` — `extractGeomColumnName`
unwraps the `st_geomfrombinary` call to find the row-side variable. A direct
variable arg (no wrap) is also accepted for unit tests and any future planner
shape change.

**What was tried, and why:**
- Catalog config (`geomesa.geometry.columns`) was vestigial — removing it
  simplified the connector with zero loss of expressive power, since every
  spatially-meaningful column already has companions.
- Per-table Iceberg property was considered, rejected: introduced a second
  source of truth that could diverge from the actual schema.

---

## § 10 — Truncate-string pushdown: invisible in EXPLAIN, and a Range.equal-vs-Range.range pitfall

Two empirical findings from debugging the regions-count mismatch
(`Z2PruningIT.regionsCountMatchesUnprunedBaseline` getting 92 of 98
expected rows).

### Truncate-string partition projection is NOT surfaced in EXPLAIN

Trino's iceberg connector projects truncate-string partition predicates
at split-generation time, but the projected predicate does not appear in
EXPLAIN's `constraint on [...]` block. Only identity-partitioned columns
show there. For a query like
`WHERE __geom_z2__ = '9a00000000000000'` on a `truncate(2)` partition,
EXPLAIN shows only:
```
filterPredicate = (geom_z2 = varchar '9a00000000000000')
```
with no `constraint on [...]`. The pushdown IS happening — we verified
via EXPLAIN ANALYZE that the scan reads ~28 of 228 splits (≈12%) on
matching data. The observable proof of pruning is the reduced
`Splits: N` / `Input: M rows` line, not the `constraint on` block.

Implication: tests must assert pruning via EXPLAIN ANALYZE, not EXPLAIN.
`Z2PruningIT` and `MultiGeomIT` use a `scanInputRows()` helper that
parses `Input: N rows` from EXPLAIN ANALYZE and asserts
`scanned < table_total`.

### Range.equal vs Range.range(x, x, closed, closed) are NOT interchangeable

`Range.range(start, end, true, true)` with `start == end` is
semantically equivalent to `Range.equal(start)` in Trino's `RangeSet`
API. But when both are pushed alongside the four `__<X>_bbox__`
sub-field domains AND projected through the truncate-string partition
transform, Iceberg's combined manifest-stat evaluator handles them
DIFFERENTLY:

- `Range.equal(start)` → bbox + partition AND-combine correctly, file
  kept iff per-column stats allow.
- `Range.range(start, start, true, true)` → bbox + partition combination
  prunes files where the individual stat checks all pass. Concretely:
  the partition-80 file containing the matching rows has
  `xmax ∈ [-112, -77.74]`, `xmin ∈ [-113, -79.54]`, `ymax ∈ [42, 47.84]`,
  `ymin ∈ [41.6, 47.07]` — every individual `bbox.<sub> op V` check
  passes, but `Range.range(x, x)` on `__geom_xz2__` combined with all
  four bbox predicates makes Iceberg drop the file. Bisecting reveals
  that any 3-of-4 bbox-pred combination keeps it; only all 4 plus the
  range-singleton triggers the over-prune.

Workaround (current code in `SpatialConnectorMetadata.applyFilter` XZ2
case): when `xz2RangesAtReferenceHex` returns a singleton entry
(`r[0].equals(r[1])`), push it as `Range.equal(slice)`; otherwise push
`Range.range(slice, slice, true, true)` for wide ranges. The fix is
narrow: it doesn't touch Z2 (which always pushes wide ranges), and the
public `xz2RangesAtReferenceHex` API still returns String[2] pairs so
the singleton-vs-wide distinction is a connector-side decision.

Worth filing upstream eventually. The bug is reproducible in Trino 476
+ Iceberg-core 1.9.1; whether it's in Trino's `Domain` → Iceberg
`Expression` conversion or in Iceberg's `Truncate.project` for the
combination of EQ + struct sub-field stats remains unverified.

### The bbox sub-field pruning still earns its keep (empirical)

Despite the combined-pred pitfall above, the bbox sub-field pruning is
substantially beneficial. On tdrive (1.93M rows, 205 partition files):

| Variant                            | Splits | Rows scanned | Physical I/O |
|------------------------------------|--------|--------------|--------------|
| Spatial overlay (XZ2 + bbox subs)  |     13 |        1.93M |      9.58 MB |
| XZ2 partition pushdown only        |     35 |        2.12M |     17.56 MB |
| Iceberg full scan baseline         |    205 |        2.13M |     12.47 MB |

XZ2 pruning does the heavy lift (205 → 35 splits). The bbox sub-field
stats cut splits further by ~62% (35 → 13) and physical I/O by ~45%
(17.56 → 9.58 MB). The extra pruning exists only because
`enable_pyiceberg_nested_metrics()` keeps
`write.metadata.metrics.column.__<X>_bbox__.{xmin,ymin,xmax,ymax}=full`
in effect — without that runtime patch, PyIceberg 0.11.1's hardcoded
nested-field downgrade would drop those leaf min/max values to COUNTS,
leaving Iceberg nothing to prune on.
