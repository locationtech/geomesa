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

.. _trino_bbox_page_filter:

Bounding-box page filtering
---------------------------

``geomesa.spatial.bbox-page-filter`` controls whether the connector injects a bounding-box filter for spatial predicates. The
filter can eliminate rows based on fast bounding box comparisons, without having to decode the full binary geometry
value. But when disabled, the bounding box columns can be skipped, resulting in higher throughput. Thus, the filter is most
useful when it eliminates many rows, for example when data is coarsely partitioned. Otherwise, regular manifest and
file-level pruning may be sufficient. See :ref:`trino_design` for more details on query pruning.
