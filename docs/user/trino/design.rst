.. _trino_design:

Trino Data Store Design
=======================

Architecture
------------

.. code-block:: text

    ┌────────────────────────────────────────────────────────────────────────┐
    │  Trino cluster (481+)                                                  │
    │                                                                        │
    │  catalog: iceberg          catalog: spatial_iceberg                    │
    │  (stock Iceberg connector) (SpatialConnector wrapping Iceberg)         │
    │                             └── SpatialConnectorMetadata               │
    │                                  applyFilter() → bbox + Z2 TupleDomain │
    │                                                                        │
    │  CQL clients ──► geomesa-trino-datastore ──► TrinoFilterToSQL          │
    │                 (CQL filter ──► pure row-level ST_* SQL, column-first) │
    └──────────────┬──────────────────────────────────────┬──────────────────┘
                   │                                      │
           ┌───────▼──────────┐                  ┌────────▼─────────────────┐
           │  Iceberg catalog │                  │  S3-compatible store     │
           │  (REST or Glue)  │                  │  s3://warehouse/ Parquet │
           └──────────────────┘                  └──────────────────────────┘

Two catalogs are intentional:

* ``iceberg`` — stock connector, no spatial awareness; used as a baseline.
* ``spatial_iceberg`` — wrapper that intercepts ``applyFilter()`` to inject
  TupleDomain constraints on the ``__<geom>_bbox__`` struct sub-fields **and** a
  ``SortedRangeSet`` over ``__<geom>_z2__`` partition values. Iceberg uses both for
  manifest-list and per-file pruning at scan-planning time.

How the Three Pruning Layers Compose
------------------------------------

Layer 1: Z2/XZ2 Partition Pruning (Truncate-String Manifest Pruning)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Tables are ``truncate(N_chars)``-partitioned on ``__<geom>_z2__`` (or ``__<geom>_xz2__``
for non-point datasets, where ``<geom>`` is the geometry column's name — see
:ref:`trino_companion_columns`), a VARCHAR holding the 16-char zero-padded lowercase
unsigned hex of the upstream GeoMesa SFC index. For Z2, the index is
``Z2SFC.index(lon, lat) << 2`` — a left-shift by 2 to rotate the lat/lon
hemisphere bits up into the top hex char (Z2SFC reserves bits 62 and 63,
so the shift wastes nothing). At ``N_chars = 1`` the 16 possible partition
values map to a 4×4 grid of hemispheric quadrants. For XZ2, the index is
``XZ2SFC(g=12).index(envelope)`` with no shift
(sequence codes don't carry geographic info in their high bits in a way
a fixed shift could exploit). SFC outputs are always non-negative, so
unsigned-hex byte-lex order already matches numeric order — no Calrissian
sign-flip is applied. The partition spec's ``TruncateTransform`` keeps the
first ``N_chars`` of the hex string, so the effective resolution is
``N = 4 × N_chars`` bits and is read from the partition spec at discovery
time — see :ref:`trino_partitioning`.

When a spatial query arrives at ``SpatialConnectorMetadata.applyFilter()``, the
connector extracts the query envelope from the predicate (either an ``ST_*``
function call or a 4-pattern bbox-struct comparison emitted by the data store)
and expands it into a tight cover of hex ranges via
``Z2Transform.z2RangesAtReferenceHex`` (delegates to ``Z2SFC.ranges``) or
``XZ2Transform.xz2RangesAtReferenceHex`` (delegates to ``XZ2SFC(g=12).ranges``).
Because SFC outputs are non-negative, the unsigned-hex endpoints are
monotonic in byte-lex order — a single range per SFC range, no
midpoint-split required.

The cover is pushed as a ``SortedRangeSet<VARCHAR>`` over the partition column;
Iceberg projects each range through the truncate-string transform to a
partition prefix predicate and skips whole manifests whose
``partition_summaries`` don't intersect — **without opening them** — the
metadata-side win that scales with table size. The truncate-string
projection is NOT surfaced in EXPLAIN's ``constraint on [...]`` block; verify
pruning via EXPLAIN ANALYZE's reduced scan-input row count (see
:ref:`trino_verify_pruning`).

Layer 2: Per-File ``__<geom>_bbox__`` Column-Stat Pruning
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each row carries ``__<geom>_bbox__`` (an Iceberg struct of ``xmin``, ``ymin``, ``xmax``,
``ymax`` as float32). Iceberg writes per-leaf Parquet statistics for the four
bounds. The connector also pushes four REAL-typed domains (``xmax >= envMinX``,
``xmin <= envMaxX``, ``ymax >= envMinY``, ``ymin <= envMaxY``) into the Iceberg
delegate's TupleDomain. Both connectors evaluate these against per-file column
stats; files whose bbox can't intersect the query envelope are skipped at
planning time.

At identity-Z2 partitioning, this layer converges with Layer 1 on the same file
set — files in non-overlapping Z2 cells have non-overlapping bbox stats, so
benchmark deltas between the two connectors read ~0% on rectangular queries:
both land at the same file count via different mechanisms.

Layer 3: Connector-Side Page-Source bbox Filter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The data store emits only row-level ``ST_*`` predicates (no ``CASE WHEN``), so the
row-level saving that shortcut used to provide is recovered *inside the connector*, where
it applies uniformly to data store queries and hand-written Trino SQL alike. When
``geomesa.spatial.bbox-page-filter`` is enabled (the default), ``SpatialPageSourceProvider``
wraps the Iceberg page source with ``BboxFilteringPageSource``. It reads only the cheap
``__<geom>_bbox__`` sub-field columns (the REAL domains injected in Layer 2, carried to the
worker in the table handle's unenforced predicate) and filters rows **before** the geometry
WKB is decoded, in one of two modes chosen per query:

* **Reject pre-filter** — for any spatial predicate, drops rows whose bbox cannot overlap
  the query envelope and passes the survivors to the engine's exact ``ST_*`` residual. A
  sound, non-destructive pre-filter: only rows failing a *necessary* overlap condition are
  dropped, so no matching row is removed and the exact predicate still runs on every
  survivor. Its win is the WKB decode it avoids on rejected rows.
* **Short-circuit** — for a rectangle ``ST_Intersects`` on a Z2/point geometry column the
  bbox proves the result exactly, so ``applyFilter()`` claims the predicate enforced
  (replacing only the ``st_intersects`` node with ``TRUE`` — never ``is_visible`` or any
  other conjunct) and the page source becomes authoritative: rows whose bbox is inside are
  **accepted with no decode**, rows outside are rejected, and only the thin
  envelope-boundary shell is decoded and tested exactly. For point columns the stored bbox
  is degenerate (``xmin == xmax``, ``ymin == ymax``), so the classifier reads two coordinate
  columns and the box test collapses to a point-in-range check.

Directional float32 rounding keeps both modes exact: the reject box is rounded outward and
the accept box inward, so any row a nearest-rounded float32 bbox could misclassify falls
through to the exact test. The mode each predicate uses:

.. list-table::
    :header-rows: 1
    :widths: 30 22 48

    * - Predicate (emitted by the data store / recognized in SQL)
      - Page-source mode
      - Soundness
    * - ``ST_Intersects(geom, axis-aligned rectangle)`` on a point/Z2 column
      - short-circuit
      - bbox⊆rect ⇒ geom⊆rect ⇒ intersects (the rectangle IS its own envelope); the
        boundary shell falls to the exact test
    * - ``ST_Intersects`` on a non-rectangular polygon, or on a non-point (XZ2) column
      - reject-only
      - bbox⊆env(polygon) does NOT imply intersection (holes, concavity); the exact
        ``ST_Intersects`` runs on survivors
    * - ``ST_Within`` / ``ST_Contains`` / ``ST_Crosses`` / ``ST_Touches`` /
        ``ST_Overlaps`` / ``ST_Equals``
      - reject-only
      - each implies a non-empty intersection ⇒ bbox-overlap is a valid necessary
        prefilter; the exact predicate decides membership on survivors
    * - ``DWITHIN`` — emitted as ``ST_Intersects(geom, outer rectangle) AND ST_Distance(...) ≤ d``
      - reject-only (on the outer rectangle)
      - the outer rectangle (reference expanded by ``d``) is a necessary bound on the
        distance neighborhood; the exact spherical ``ST_Distance`` runs on survivors
    * - ``ST_Disjoint`` / ``BEYOND`` (distance > d)
      - none
      - matching rows lie *outside* the query neighborhood — a bbox-overlap prefilter would
        prune the answer, so no bbox pushdown is derived

Doing this in the connector rather than as ``CASE WHEN`` SQL is deliberate: it keeps the
data store's emitted SQL pure ``ST_*`` (so plans are identical however a query arrives),
and it sidesteps Trino's optimizer distributing an ``OR`` form over ``AND`` — which would
evaluate the expensive predicate up to 4× per row (a 3.3× wall-clock slowdown was measured
with the old SQL forms). The filter is a pure performance layer: disabling it
(``geomesa.spatial.bbox-page-filter=false``) changes runtime, never results. See
:ref:`trino_configuration` for the cost/benefit and when to disable.

.. _trino_partitioning:

Partitioning and Resolution
---------------------------

Table writers set the effective resolution N per geom column by configuring the
partition spec's ``TruncateTransform(width=ceil(N/4))`` on the hex-encoded VARCHAR
partition column. Each retained hex character encodes 4 bits, so
``N = 4 × N_chars``.

``Z2 bits`` is the **total** spatial-index width (per-axis = total/2). Cell size
is per-axis at the equator: longitude = 360°/2^(bits/2), latitude =
180°/2^(bits/2). The same bit-width convention applies to both Z2 and XZ2. As a
rule of thumb, coarse resolutions (~8 bits, ≥ 11° cells) suit sparse
continent-scale data where finer partitioning would produce single-row files,
while dense city-scale point data benefits from 16–20 bits (0.35°–1.4° cells),
which unlocks the ``ST_Intersects`` shortcut on tight query envelopes.

Point-only geometry columns use ``__<geom>_z2__`` (Z2 cell index of the geometry's
bbox centroid). Non-point geometry columns use ``__<geom>_xz2__`` (XZ2 cell index —
the smallest quadtree cell whose footprint fully contains the geometry's envelope).
The connector auto-detects which companion is present per geometry column and routes
the appropriate range generator. (Non-point data needs XZ2 because a centroid's Z2
cell doesn't bound the geometry's extent — pruning on it would drop rows whose
geometry overlaps the query envelope from a neighboring cell.)

Effective resolution is encoded in each table's partition spec
(``truncate(__<geom>_z2__, N/4)`` on the VARCHAR hex column). The connector
derives ``N = 4 × width`` from the spec at discovery time; no separate
storage property carries it. The reader-side fallback
``GeoMesaColumnCatalog.DEFAULT_BITS = 12`` applies only when the partition
spec is missing or malformed.

.. _trino_companion_columns:

Companion Columns and Naming Convention
---------------------------------------

Each geometry column in a table is paired with a group of *companion columns* that
carry its per-row bbox and spatial index. Throughout this documentation,
``__<geom>_bbox__``, ``__<geom>_z2__``, and ``__<geom>_xz2__`` use ``<geom>`` as a
placeholder for the geometry column's *name*: for a geometry column named ``geom``
(the common convention for single-geometry tables), the companions are
``__geom_bbox__`` plus ``__geom_z2__`` (or ``__geom_xz2__``).

Discovery is by naming convention alone: a VARBINARY column ``X`` is treated as a
geometry column iff at least one of ``__X_bbox__``, ``__X_z2__``, or ``__X_xz2__``
exists in the same table — no table property or catalog configuration is needed. A
geometry column carries one spatial-index companion; in the degenerate case where
both ``__X_z2__`` and ``__X_xz2__`` are present, the connector prefers XZ2 (its
cell set is correct for all geometry types).

Tables with multiple geometry columns carry one companion group per geometry
column. For example, a track-observation table with a ``location`` column (point
geometries) and a ``path`` column (linestring geometries) would have:

.. code-block:: text

    location   VARBINARY   ─┬─ __location_z2__    VARCHAR (partition column)
                            └─ __location_bbox__  ROW(xmin, ymin, xmax, ymax)
    path       VARBINARY   ─┬─ __path_xz2__       VARCHAR (partition column)
                            └─ __path_bbox__      ROW(xmin, ymin, xmax, ymax)

The connector discovers each group independently and prunes on whichever geometry
column(s) a query filters against.

.. _trino_schema:

Table Schema
------------

Tables are truncate-partitioned on their spatial-index companion columns
(``__<geom>_z2__`` or ``__<geom>_xz2__``; VARCHAR hex column, ``truncate(N_chars)``),
and where applicable on ``month(dtg)`` or ``year(dtg)``. The Z2 bit resolution is
per-table; see :ref:`trino_partitioning`.

Common columns across all spatial tables, shown for a single geometry column
named ``geom``:

.. list-table::
    :header-rows: 1
    :widths: 20 25 55

    * - Column
      - Type
      - Notes
    * - ``__fid__``
      - VARCHAR (required)
      - SimpleFeature ID
    * - ``geom``
      - VARBINARY
      - WKB geometry, identical on both catalogs; wrap with ``ST_GeomFromBinary(geom)`` for spatial functions
    * - ``dtg``
      - TIMESTAMP WITH TIME ZONE
      - event/observation time (where applicable)
    * - ``__geom_bbox__``
      - ROW(xmin, ymin, xmax, ymax FLOAT)
      - per-row bbox companion; Parquet column stats drive file-level pruning
    * - ``__geom_z2__``
      - VARCHAR (partition column)
      - spatial-index companion: 16-char zero-padded lowercase unsigned hex of
        ``Z2SFC.index(centroid) << 2`` at 31 bits/axis (shift exposes lat/lon hemisphere bits
        in the top hex char so ``truncate(1)`` already discriminates by hemispheric quadrant);
        ``truncate(N_chars)``-partitioned for manifest pruning
