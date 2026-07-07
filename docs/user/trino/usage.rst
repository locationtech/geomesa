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

Direct-SQL consumers (JDBC, BI tools) write their own SQL — these patterns let them
adopt the same optimization shapes manually. The examples below run against a table
whose geometry column is named ``geom``, so its companions are ``__geom_bbox__`` and
``__geom_z2__``; substitute your geometry column's name per the convention in
:ref:`trino_companion_columns` (e.g. a ``path`` column is pruned via
``__path_bbox__`` / ``__path_xz2__``).

.. code-block:: sql

    -- BBOX: bbox-overlap on the struct fields. Both connectors prune files via
    -- per-leaf Parquet stats on __geom_bbox__; SI also pushes Z2 partition pruning.
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE "__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
      AND "__geom_bbox__".ymax >=  37 AND "__geom_bbox__".ymin <=  45;

    -- ST_Intersects with row-level shortcut. The leading bbox-overlap conjunct
    -- triggers file-level + Z2 pruning; the CASE WHEN short-circuits ST_Intersects
    -- (and the WKB decode that precedes it) for rows whose bbox is fully inside.
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE ("__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
           AND "__geom_bbox__".ymax >= 37 AND "__geom_bbox__".ymin <= 45)
      AND CASE WHEN "__geom_bbox__".xmin >= -80 AND "__geom_bbox__".xmax <= -70
                AND "__geom_bbox__".ymin >= 37  AND "__geom_bbox__".ymax <= 45
               THEN TRUE
               ELSE ST_Intersects(ST_GeomFromBinary(geom), ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'))
          END;

    -- ST_Within(geom, axis-aligned-rectangle): equivalent to bbox-contained, no
    -- row-level ST_Within needed. Trino consolidates the two conjuncts into BETWEEN.
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE ("__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
           AND "__geom_bbox__".ymax >= 37 AND "__geom_bbox__".ymin <= 45)
      AND ("__geom_bbox__".xmin >= -80 AND "__geom_bbox__".xmax <= -70
           AND "__geom_bbox__".ymin >= 37 AND "__geom_bbox__".ymax <= 45);

    -- DWITHIN: outer-bbox-overlap (file pruning) + CASE WHEN inner-inscribed-rect
    -- (sufficient for distance ≤ d) ELSE exact spherical distance. The inner
    -- rectangle's corners land at distance 0.9 × d from ref; rows whose bbox fits
    -- inside it skip ST_Distance entirely.
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE ("__geom_bbox__".xmax >= -77.94 AND "__geom_bbox__".xmin <= -76.14
           AND "__geom_bbox__".ymax >= 37.91 AND "__geom_bbox__".ymin <= 39.91)
      AND CASE WHEN "__geom_bbox__".xmin >= -77.66 AND "__geom_bbox__".xmax <= -76.42
                AND "__geom_bbox__".ymin >= 38.18 AND "__geom_bbox__".ymax <= 39.64
               THEN TRUE
               ELSE ST_Distance(
                   to_spherical_geography(ST_GeomFromBinary(geom)),
                   to_spherical_geography(ST_GeometryFromText('POINT (-77.04 38.91)'))
               ) <= 100000
          END;

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

    -- Check the three pruning layers are active. EXPLAIN shows layers 2 & 3
    -- directly; layer 1 (truncate-string partition projection) is NOT surfaced
    -- in `constraint on [...]` — use EXPLAIN ANALYZE and confirm
    -- `Splits: N` and `Input: M rows` are much smaller than the table's totals.
    --   1. EXPLAIN ANALYZE shows reduced scan-input rows/splits — Z2/XZ2 partition pushdown
    --   2. `geom_bbox_xmax >= ... AND geom_bbox_xmin <= ...` in filterPredicate — bbox-stat pushdown
    --   3. (Optional) `CASE WHEN ... THEN TRUE ELSE st_intersects(...)` for row-level shortcut
    EXPLAIN
    SELECT COUNT(*) FROM observations
    WHERE ("__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
           AND "__geom_bbox__".ymax >= 37 AND "__geom_bbox__".ymin <= 45)
      AND CASE WHEN "__geom_bbox__".xmin >= -80 AND "__geom_bbox__".xmax <= -70
                AND "__geom_bbox__".ymin >= 37  AND "__geom_bbox__".ymax <= 45
               THEN TRUE
               ELSE ST_Intersects(ST_GeomFromBinary(geom), ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'))
          END;

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
