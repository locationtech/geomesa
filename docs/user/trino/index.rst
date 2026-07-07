.. _trino_index_page:

Trino Data Store
================

GeoMesa provides GeoMesa-compatible spatial queries over Apache Iceberg tables stored as
Parquet on S3-compatible object storage, served by a custom Trino connector
(``spatial_iceberg``) and a Trino-backed GeoMesa data store. Three pruning layers compose
end-to-end:

* **Z2/XZ2 truncate-string partition pruning** at the connector level (skip whole
  manifests via Iceberg's projection of pushed VARCHAR ranges through
  ``truncate(N_chars)`` on ``__<geom>_z2__`` / ``__<geom>_xz2__``).
* **Per-file** ``__<geom>_bbox__`` **column-stat pruning** at planning time (works for both
  the spatial connector and stock Iceberg).
* **Row-level CASE WHEN bbox-contained shortcut** in the SQL emitted by the GeoMesa
  data store (skips ``ST_Intersects``/``ST_Distance`` for rows whose bbox is fully inside
  the query envelope).

For axis-aligned rectangular WITHIN queries the row-level test is eliminated entirely
— the bbox-contained predicate is exactly equivalent to ``ST_Within``.

``<geom>`` above is a placeholder for the geometry column's name; each geometry
column in a table gets its own companion group, so multi-geometry tables carry
several (see :ref:`trino_companion_columns`).

GeoMesa Trino consists of two modules:

* ``geomesa-trino-plugin`` — a Trino plugin (fat JAR) providing the ``spatial_iceberg``
  connector, which wraps the stock Iceberg connector and injects spatial pushdown in
  ``applyFilter()``.
* ``geomesa-trino-datastore`` — a client-side GeoTools data store over Trino JDBC, which
  translates CQL filters into optimized spatial SQL.

.. note::

    The module is currently scoped as a **read-only** query engine — tables are created
    and populated by external writers, in particular the :doc:`FileSystem data store </user/filesystem/index>`.

.. toctree::
    :maxdepth: 1

    design
    install
    usage
    security
