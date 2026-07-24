.. _trino_parameters:

Trino Data Store Parameters
===========================

Data Store Parameters
---------------------

The Trino data store takes the following parameters (required parameters are marked with ``*``):

================================== ====== ========================================================================================
Parameter                          Type   Description
================================== ====== ========================================================================================
``trino.host *``                   String Trino coordinator host
``trino.port *``                   Int    Trino coordinator port
``trino.schema``                   String Trino schema (default ``spatial``)
``trino.user``                     String Trino session user
``geomesa.security.auths-secret``  String Shared secret presented to Trino as the 'secret' extra credential; must match the
                                          catalog's secret
``geomesa.security.*``                    See :ref:`trino_security`
================================== ====== ========================================================================================

Programmatic Access
-------------------

An instance of the data store can be obtained through the normal GeoTools discovery
methods, assuming that the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("trino.host", "trino-coordinator");
    parameters.put("trino.port", 8080);
    parameters.put("trino.schema", "spatial");
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

.. _trino_sql_patterns:

SQL Patterns for Direct-SQL Consumers
-------------------------------------

Direct-SQL consumers can still make spatial queries using standard spatial ``ST_*`` functions. The
examples below run against a table whose geometry column is named ``geom`` - see :ref:`trino_companion_columns`
for a description of the additional fields.

.. code-block:: sql

    -- Standard intersects query
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE ST_Intersects(ST_GeomFromBinary(geom),
          ST_GeometryFromText('POLYGON ((-80 37, -70 37, -70 45, -80 45, -80 37))'));

    -- DWITHIN query
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE ST_Intersects(ST_GeomFromBinary(geom),
            ST_GeometryFromText('POLYGON ((-77.94 37.91, -76.14 37.91, -76.14 39.91, -77.94 39.91, -77.94 37.91))'))
      AND ST_Distance(
              to_spherical_geography(ST_GeomFromBinary(geom)),
              to_spherical_geography(ST_GeometryFromText('POINT (-77.04 38.91)'))
          ) <= 100000;

    -- Bounding boxes can be queried directly, note that they are stored at float32 precision,
    -- so may not provide 100% accurate results
    SELECT COUNT(*) FROM spatial_iceberg.spatial.observations
    WHERE "__geom_bbox__".xmax >= -80 AND "__geom_bbox__".xmin <= -70
      AND "__geom_bbox__".ymax >=  37 AND "__geom_bbox__".ymin <=  45;

Note that geometry columns are defined as ``VARBINARY`` (encoded WKB), not Geometry types, thus every spatial function call
must wrap the column with ``ST_GeomFromBinary(geom)``, as shown above.

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
