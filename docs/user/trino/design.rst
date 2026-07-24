.. _trino_design:

Trino Data Store Design
=======================

The Trino data store brings support for spatial queries to `Apache Iceberg <https://iceberg.apache.org/>`_ tables stored as
Parquet on S3-compatible object storage. It accomplishes this by adding spatial columns to the Parquet schema, which can
then be partitioned and transparently queried using standard Iceberg transforms.

.. _trino_companion_columns:

Companion Columns and Naming Convention
---------------------------------------

Each geometry column in a table is paired with a group of *companion columns* that carry its per-row bounding box and spatial
index (derived from a space-filling curve). The names for these columns are prefixed with ``__`` in order to denote their
internal-facing intent, and they are transparently hidden from the end user when queried through GeoTools. The columns consist
of a bounding box containing four floats (latitude and longitude min/max), and a hex-encoded Z-index space-filling curve value.
For example, a table with a ``location`` column (point geometries) and a ``path`` column (linestring geometries) would have:

.. code-block:: text

    location                 VARBINARY (encoded WKB geometry)
    ─┬─ __location_z2__      VARCHAR (hex-encoded z-value)
     └─ __location_bbox__    ROW[xmin, ymin, xmax, ymax]
    path                     VARBINARY (encoded WKB geometry)
    ─┬─ __path_xz2__         VARCHAR (hex-encoded z-value)
     └─ __path_bbox__        ROW[xmin, ymin, xmax, ymax]

Spatial Partitioning
--------------------

Spatial partitioning is accomplished used an Iceberg ``truncate`` transform on the Z-index column. The spatial resolution of the
partitions is controlled by the length of the truncate operation. Each hex-encoded character represents 4 bits of total
spatial resolution, e.g. ``truncate(1)`` would create a 4x4 spatial grid (2 bits per axis). Spatial resolution should be
determined carefully based on the density of data (which affects how many files are in each partition), as well as typical
query patterns.

Query Partition Pruning
-----------------------

There are three layers of spatial filter pruning that compose end-to-end to accelerate queries:

* **Z2/XZ2 manifest pruning** - Using the spatial Z-index column, whole groups of files can be excluded from the query.
* **Bounding box file pruning** - Individual files and row groups can be pruned out based on the file-level column bounds
  for each bounding box field.
* **Row level bounding box filtering** - The GeoMesa spatial connector injects a special page source
  that will filter individual rows based on their bounding box, avoiding expensive geometry decoding and operations
  where possible. This can be disabled if desired - see :ref:`trino_configuration`.

The GeoMesa connector applies all three layers of pruning transparently, based on the spatial predicates in the query.
