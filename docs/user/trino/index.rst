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
* **Connector-side page-source bbox filtering** (``geomesa.spatial.bbox-page-filter``,
  on by default): before a surviving row's geometry WKB is decoded, the connector
  rejects rows whose ``__<geom>_bbox__`` cannot overlap the query envelope, and — for a
  rectangle ``ST_Intersects`` on a point/Z2 column — accepts rows whose bbox is fully
  inside without decoding at all. See :ref:`trino_configuration`.

The connector derives all three layers from the same ``ST_*`` call, so a CQL query
through the data store and the equivalent hand-written Trino SQL get identical plans.
The data store's ``TrinoFilterToSQL`` emits only row-level ``ST_*`` predicates; the
bbox/Z2 pruning and the row-level bbox filter are the connector's responsibility.

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
    configuration
    usage
    security
