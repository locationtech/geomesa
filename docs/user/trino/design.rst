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
    │                 (CQL filter ──► bbox-overlap + CASE WHEN shortcut SQL) │
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

Layer 3: Row-Level CASE WHEN bbox-Contained Shortcut
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For surviving rows, the GeoMesa data store's ``TrinoFilterToSQL`` emits SQL that
short-circuits the expensive geometry test when the row's bbox is fully inside
the query envelope. The form differs by spatial filter type:

.. list-table::
    :header-rows: 1
    :widths: 20 45 35

    * - CQL filter
      - Emitted SQL pattern
      - Soundness
    * - ``INTERSECTS(geom, axis-aligned rectangle)``
      - ``(bbox-overlap) AND CASE WHEN bbox-contained THEN TRUE ELSE ST_Intersects(geom, rect) END``
      - bbox⊆rect ⇒ geom⊆rect ⇒ ST_Intersects=TRUE (sufficient; the rectangle IS its envelope)
    * - ``INTERSECTS(geom, non-rectangular polygon)``
      - ``(bbox-overlap) AND ST_Intersects(geom, polygon)``
      - no shortcut: bbox⊆env(polygon) does NOT imply intersection (holes, concavity)
    * - ``WITHIN(geom, axis-aligned rectangle)``
      - ``(bbox-overlap) AND CASE WHEN bbox-in-shrunk-rect THEN TRUE ELSE ST_Within(geom, rect) END``
      - WITHIN is boundary-exclusive and the stored bbox is float32, so the shortcut
        rectangle is shrunk by two float ulps per side (containment then proves the
        geometry is strictly interior); boundary-adjacent rows take the exact test
    * - ``WITHIN(geom, non-rectangular polygon)``
      - ``(bbox-overlap) AND ST_Within(geom, polygon)``
      - bbox⊆env(polygon) does NOT imply geom⊆polygon
    * - ``DWITHIN(geom, ref, d)``
      - ``(outer-bbox) AND CASE WHEN bbox-in-inner-inscribed-rect THEN TRUE ELSE ST_Distance(...) ≤ d END``
      - outer bbox = env(ref) expanded by d (covers extended references end to end);
        the inscribed-rect shortcut applies to point references only — for lines and
        polygons the exact spherical check runs via the planar nearest-points pair
        (Trino's spherical ``ST_Distance`` is point-only)
    * - ``BBOX(geom, env)``
      - same as ``INTERSECTS(geom, envelope-rectangle)``
      - a bare float32 bbox-overlap is not exact (the stored bbox is rounded to
        nearest, admitting rows up to ½ ulp outside the envelope), so BBOX takes the
        rectangle-intersects shape: overlap prefilter + shrunk-contained shortcut +
        exact ``ST_Intersects`` fallback
    * - ``CROSSES`` / ``TOUCHES`` / ``OVERLAPS`` / ``EQUALS``
      - ``(bbox-overlap) AND ST_<op>(geom, g)``
      - each implies a non-empty intersection ⇒ bbox-overlap is a valid necessary prefilter
    * - ``CONTAINS(geom, g)``
      - ``(bbox-covers) AND ST_Contains(geom, g)``
      - geom ⊇ g ⇒ bbox(geom) ⊇ env(g) (necessary); reversed operands reuse the WITHIN paths
    * - ``DISJOINT`` / ``BEYOND``
      - exact ``ST_Disjoint`` / spherical distance > d, **no prefilter**
      - the matching rows lie outside the query neighborhood — an overlap prefilter would prune the answer

CASE WHEN — not OR. Trino's optimizer distributes OR over AND, causing the
expensive predicate to evaluate up to 4× per row (3.3× wall-clock slowdown
measured). CASE WHEN survives the optimizer intact and short-circuits cleanly.

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
