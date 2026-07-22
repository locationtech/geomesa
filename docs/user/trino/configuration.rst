.. _trino_configuration:

Trino Connector Configuration
=============================

The ``spatial_iceberg`` connector is configured through **catalog properties** in its
``.properties`` file (see :ref:`trino_install`). Any property that is not in a
``geomesa.*`` namespace is passed through unchanged to the stock Iceberg connector the
plugin wraps, so all of Iceberg's own catalog properties are available as-is. The
GeoMesa-specific properties below are consumed by the plugin and stripped before the
config reaches the Iceberg delegate.

.. list-table::
    :header-rows: 1
    :widths: 32 12 12 44

    * - Catalog property
      - Default
      - Required
      - Description
    * - ``geomesa.spatial.bbox-page-filter``
      - ``true``
      - no
      - Enables connector-side, pre-decode bbox filtering in the page source (see below).
    * - ``geomesa.security.*``
      - —
      - no
      - Trino-layer row-visibility enforcement; see :ref:`trino_security`.

.. _trino_bbox_page_filter:

Bounding-box page filtering
---------------------------

``geomesa.spatial.bbox-page-filter`` controls whether the connector wraps the Iceberg
page source with a bounding-box filter that runs **before** each row's geometry WKB is
decoded. The filter reads only the cheap ``__<geom>_bbox__`` sub-field columns (which the
connector already injects as REAL domains for per-file stat pruning, and which ride to the
worker in the table handle's unenforced predicate) and operates in one of two modes,
chosen per query:

* **Reject pre-filter** (all spatial predicates) — drops any row whose bbox *cannot*
  overlap the query envelope, then hands the survivors to the engine's exact ``ST_*``
  residual. This is a sound, non-destructive pre-filter: only rows failing a *necessary*
  overlap condition are removed, so no matching row is ever dropped and the engine's exact
  predicate still runs on every survivor. It saves the WKB decode on the rejected rows for
  any spatial predicate — including non-rectangular geometries, ``ST_Contains``, and the
  outer envelope of a ``DWITHIN`` distance query.

* **Short-circuit** (rectangle ``ST_Intersects`` on a Z2/point geometry column) — here the
  connector can claim the predicate *enforced*, so the page source is the authoritative
  filter: rows whose bbox lies fully inside the query rectangle are **accepted** with no
  WKB decode, rows outside are **rejected**, and only rows on the thin envelope boundary
  fall through to an exact decode-and-test. For point geometry columns the stored bbox is
  degenerate (``xmin == xmax``, ``ymin == ymax``), so the test reads two coordinate columns
  and collapses to a point-in-range check.

Both modes use directional float32 rounding — the reject box is rounded outward and the
accept box inward — so any row a nearest-rounded float32 bbox could misclassify falls
through to the exact test, and the result is identical to the engine's exact predicate.
Non-spatial queries carry no bbox domains and delegate to Iceberg unchanged.

When to disable
~~~~~~~~~~~~~~~

The filter's value is inversely proportional to spatial partition quality. On a
well-``truncate``-partitioned Z2/XZ2 table, Layer 1 and Layer 2 pruning
(see :ref:`trino_design`) already eliminate most non-matching rows before the page source
runs, so the extra bbox-column read can cost more than the decodes it saves — this is most
noticeable for ``DWITHIN``, where the geometry must be materialized for the exact distance
test regardless. On a coarsely-partitioned table the row-level rejection catches what file
pruning misses and is a clear win. Set ``geomesa.spatial.bbox-page-filter=false`` to turn
it off; the connector then relies on partition and per-file stat pruning alone and the
engine's exact ``ST_*`` predicate decodes every surviving row.
