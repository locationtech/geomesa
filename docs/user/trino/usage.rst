.. _trino_parameters:

Using the Trino Data Store
==========================

Data Store Parameters
---------------------

The Trino data store takes the following parameters:

.. list-table::
    :header-rows: 1
    :widths: 25 15 60

    * - Parameter
      - Required
      - Description
    * - ``trino.host``
      - yes
      - Trino coordinator host
    * - ``trino.port``
      - yes
      - Trino coordinator port
    * - ``trino.catalog``
      - no
      - Trino catalog (use ``spatial_iceberg`` for spatial pushdown)
    * - ``trino.schema``
      - no
      - Trino schema (default ``spatial``)
    * - ``namespace``
      - no
      - Namespace URI applied to type names
    * - ``trino.user``
      - no
      - Trino session user
    * - ``geomesa.security.*``
      - no
      - see :ref:`trino_security`

Programmatic Access
-------------------

An instance of the data store can be obtained through the normal GeoTools discovery
methods, assuming that the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("trino.host", "trino-coordinator");
    parameters.put("trino.port", 8080);
    parameters.put("trino.catalog", "spatial_iceberg");
    parameters.put("trino.schema", "spatial");
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

CQL consumers via the data store get the optimized SQL shapes described in
:ref:`trino_design` automatically.

.. _trino_sql_patterns:

SQL Patterns for Direct-SQL Consumers
-------------------------------------

Direct-SQL consumers (JDBC, BI tools) write their own SQL. Because the connector derives
all pruning from the ``ST_*`` call itself, hand-written SQL needs no special shapes — a
plain ``ST_*`` predicate gets the same Z2/XZ2 partition pruning, per-file bbox-stat
pruning, and page-source bbox filter as a query issued through the data store. The
examples below run against a table whose geometry column is named ``geom``, so its
companions are ``__geom_bbox__`` and ``__geom_z2__``; substitute your geometry column's
name per the convention in :ref:`trino_companion_columns` (e.g. a ``path`` column is pruned
via ``__path_bbox__`` / ``__path_xz2__``).

.. code-block:: sql

    -- ST_Intersects with an axis-aligned rectangle. The connector derives Z2/XZ2
    -- partition pruning (Layer 1) and per-file bbox-stat pruning (Layer 2) from the
    -- call; because the query is a rectangle on a point/Z2 column, its page source
    -- accepts rows whose bbox is inside without decoding the WKB (Layer 3 short-circuit).
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE ST_Intersects(ST_GeomFromBinary(geom),
          ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'));

    -- ST_Intersects with a non-rectangular polygon. Same Layer 1/2 pruning; the page
    -- source rejects rows whose bbox cannot overlap before the exact ST_Intersects runs.
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE ST_Intersects(ST_GeomFromBinary(geom),
          ST_GeometryFromText('POLYGON ((-80 37, -75 41, -70 45, -80 45, -80 37))'));

    -- ST_Within(geom, rectangle). bbox-overlap is a necessary prefilter; the exact
    -- ST_Within decides membership on the survivors.
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE ST_Within(ST_GeomFromBinary(geom),
          ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'));

    -- DWITHIN: the data store emits an outer ST_Intersects rectangle (the reference
    -- expanded by the radius) AND the exact spherical distance test. The connector
    -- derives Layer 1/2 pruning + page-source rejection from the outer rectangle; the
    -- exact ST_Distance runs only on the survivors.
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE ST_Intersects(ST_GeomFromBinary(geom),
            ST_GeometryFromText('POLYGON ((-77.94 37.91, -76.14 37.91, -76.14 39.91, -77.94 39.91, -77.94 37.91))'))
      AND ST_Distance(
              to_spherical_geography(ST_GeomFromBinary(geom)),
              to_spherical_geography(ST_GeometryFromText('POINT (-77.04 38.91)'))
          ) <= 100000;

    -- Raw bbox-overlap on the struct sub-fields is still recognized, for BI tools that
    -- cannot emit ST_*: it drives Layer 1 + Layer 2 pruning but is NOT an exact spatial
    -- test — a float32 stored bbox admits rows up to ½ ulp outside the envelope — so pair
    -- it with an ST_* predicate when exactness matters.
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE "__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
      AND "__geom_bbox__".ymax >=  37 AND "__geom_bbox__".ymin <=  45;

On **both** catalogs, ``geom`` is ``VARBINARY`` (raw WKB) — there is no Geometry-type
overlay — so every spatial function call must wrap it with ``ST_GeomFromBinary(geom)``,
as shown above. (Trino 481 removed the implicit ``VARBINARY → GEOMETRY`` coercion that
earlier releases applied, so the wrap is now mandatory, not just conventional.) The
data store emits this shape automatically.

.. _trino_verify_pruning:

Verifying the Pruning Layers
----------------------------

From any Trino client (e.g. ``trino --server <coordinator>:8080 --catalog
spatial_iceberg --schema spatial``):

.. code-block:: sql

    SHOW TABLES;
    SELECT COUNT(*) FROM observations;

    -- Check the pruning layers are active. Layer 1 (truncate-string partition
    -- projection) is NOT surfaced in `constraint on [...]` — use EXPLAIN ANALYZE and
    -- confirm `Splits: N` and `Input: M rows` are much smaller than the table totals.
    --   1. EXPLAIN ANALYZE shows reduced scan-input rows/splits — Z2/XZ2 partition pushdown
    --   2. `geom_bbox_xmax >= ... AND geom_bbox_xmin <= ...` in filterPredicate — bbox-stat pushdown
    --   3. Layer 3 (page-source bbox filter) is not a plan node: for a rectangle
    --      ST_Intersects on a point/Z2 column the `st_intersects` residual disappears
    --      entirely (claimed enforced); otherwise it remains as the residual while rows
    --      are pre-rejected in the page source — seen as EXPLAIN ANALYZE filter input
    --      dropping below the scanned row count.
    EXPLAIN
    SELECT COUNT(*) FROM observations
    WHERE ST_Intersects(ST_GeomFromBinary(geom),
          ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'));

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

Integration Tests
-----------------

Unit tests run against no external services. Integration tests (tagged
``@integration``, skipped by default) expect a running Trino at ``localhost:8080``
with the plugin loaded and the demo ``spatial.*`` tables ingested, and skip
themselves when Trino is unreachable:

.. code-block:: bash

    mvn verify -pl geomesa-trino/geomesa-trino-plugin,geomesa-trino/geomesa-trino-datastore -DskipITs=false
