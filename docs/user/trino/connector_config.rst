.. _trino_configuration:

Trino Connector Configuration
=============================

The ``spatial_iceberg`` connector is configured through catalog properties in its ``.properties`` file (see :ref:`trino_install`).
Any property that does not start with ``geomesa.*`` is passed through unchanged to the stock
`Iceberg connector <https://trino.io/docs/current/connector/iceberg.html#>`__. The following GeoMesa-specific properties are
supported:

.. list-table::
    :header-rows: 1
    :widths: 32 12 44

    * - Catalog property
      - Default
      - Description
    * - ``geomesa.spatial.bbox-page-filter``
      - ``true``
      - Enables connector-side, pre-decode bbox filtering in the page source (see below).
    * - ``geomesa.security.*``
      - —
      - Trino-layer row-visibility enforcement; see :ref:`trino_security`.
    * - ``geomesa.security.use-invoker-auths``
      - ``true``
      - Runs views in this catalog as the invoker (user) rather than the view definer (see below).

.. _trino_use_invoker_auths:

View authorizations
-------------------

``geomesa.security.use-invoker-auths`` controls whose authorizations a view's base-table scans resolve. It takes effect
only when row-visibility enforcement is enabled — that is, when a ``geomesa.security.*`` resolver is configured (see
:ref:`trino_security`). With no resolver, the connector adds no row filters and the property is ignored.

Trino views default to ``DEFINER`` security, which reads the base tables as the view *owner*. Under row-visibility
enforcement that means the injected ``WHERE`` clause is built from the owner's auth tokens, so any user permitted to
query the view would leverage the owner's auths. Left at the default ``true``, this property rewrites views
in this catalog to run as the invoker, so each base table filters rows per the auths of the user running the query.

Set it to ``false`` only when you intend views to act as a deliberate grant — the view owner exposing a curated subset of
rows to users who cannot read the base table directly — and you have confirmed the view body itself restricts the rows
appropriately. See :ref:`trino_view_security` for the full discussion, including the cases this rewrite does not cover.

.. _trino_bbox_page_filter:

Bounding-box page filtering
---------------------------

``geomesa.spatial.bbox-page-filter`` controls whether the connector injects a bounding-box filter for spatial predicates. The
filter can eliminate rows based on fast bounding box comparisons, without having to decode the full binary geometry
value. But when disabled, the bounding box columns can be skipped, resulting in higher throughput. Thus, the filter is most
useful when it eliminates many rows, for example when data is coarsely partitioned. Otherwise, regular manifest and
file-level pruning may be sufficient. See :ref:`trino_design` for more details on query pruning.

