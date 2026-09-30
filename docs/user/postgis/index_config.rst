.. _postgis_index_config:

Partitioned PostGIS Index Configuration
=======================================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
See :ref:`set_sft_options` for details on setting configuration parameters. Note that most of the general options
for GeoMesa stores are not supported by the partitioned PostGIS store, except as specified below.

.. note::

    Most configurations can be updated after a schema has been created. See below for details
    specific to each configuration.

Configuring the Default Date Attribute
--------------------------------------

The default date attribute is the attribute that will be used for sorting data into partitions. See
:ref:`set_date_attribute` for details on how to specify it.

The default date cannot be changed after the schema has been created.

Configuring Indices
-------------------

Attributes in the feature type may be marked for indexing, which will create a B-tree index on the associated
table column. See :ref:`attribute_indices` for details on how to specify indices.

After the schema has been created, additional indices can be added through ``CREATE INDEX`` statements on the
parent partition tables. See :ref:`pg_partition_table_design` for a description of the partition tables.

Configuring a Covering Date Index
---------------------------------

By default, the partition tables have a B-tree index on the default date attribute. When visibility filtering is
enabled, the hidden ``_vis`` column is always added as a trailing key, e.g. ``(dtg, _vis)``. This allows date and
visibility counts to use index-only scans while preserving PostgreSQL's ability to deduplicate repeated
date-and-visibility pairs. The default date attribute remains the leading index key, and time-based partitioning
is unchanged.

Setting ``pg.partitions.dtg-index.covering`` to ``true`` additionally carries the primary geometry using PostgreSQL's
``INCLUDE`` clause, e.g. ``(dtg, _vis) INCLUDE (geom)``. The geometry is included only if its declared type is ``Point``.
Larger geometry types are excluded to avoid exceeding the maximum index tuple size.

Covering indices can allow counts and queries whose filters and selected columns are all available in the index
to use index-only scans. For point schemas, this can include queries with time, bounding-box and visibility filters.
Avoiding heap reads also depends on PostgreSQL's visibility map, which is maintained by vacuuming. Geometry
covering increases index size and write overhead, and adding any ``INCLUDE`` column disables B-tree deduplication.
Geometry covering is disabled by default and should be enabled based on the data and query patterns.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.partitions.dtg-index.covering", "true");

This setting cannot be changed after the schema has been created. Existing schemas retain their current parent
index definitions during :ref:`postgis_partition_upgrade`; changing those definitions requires explicitly
replacing the existing indices. See the PostgreSQL
`covering index documentation <https://www.postgresql.org/docs/current/indexes-index-only-scans.html>`__ for more details.

Configuring Partition Size
--------------------------

Each feature type can be configured for a particular partition size. The partition size is specified as the number
of hours that each partition will cover, and must be a divisor of 24, i.e. ``1``, ``2``, ``3``, ``4``, ``6``,
``8``, ``12`` or ``24``.

The number of hours should be based on the expected volume of data and type of queries. Due to partition
pruning, PostGIS will not need to scan partitions that fall outside a given query window.

Partition size is configured with the key ``pg.partitions.interval.hours``.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.partitions.interval.hours", "12");

After the schema has been created, changes to the partition size can be made through the
:ref:`postgis_cli_update_schema` command, or (for schemas created or updated with GeoMesa version ``5.4.0`` or later) by directly
updating the ``geomesa_userdata`` table in Postgres. Changes will not be applied to any existing partitions. If the partition size
is **increased**, any recent partitions that would overlap with the new partition size will need to be manually dropped and the
data re-inserted into the write-ahead table in order to prevent partition range conflict errors.

Configuring Index Resolution
----------------------------

Each feature type can be configured with a number of pages per range. The partition tables use a
`BRIN <https://www.postgresql.org/docs/17/brin.html>`__ index, which is a lossy index structure.
The number of data pages stored in each index range controls how lossy, and how large the index becomes.
By default, Postgres stores 128 pages in each range. Storing fewer pages will generally make the index more
efficient, at the cost of requiring more space; however, the optimal number will depend on data characteristics
and typical query patterns.

The number of pages is configured with the key ``pg.partitions.pages-per-range``.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.partitions.pages-per-range", "64");

The index resolution cannot be changed after the schema has been created.

Configuring TOAST Tuple Target
------------------------------

PostgreSQL uses TOAST to compress or store large column values outside the main table. The key
``pg.partitions.toast-tuple-target`` configures ``toast_tuple_target`` for main and spill leaf partitions,
including tables rebuilt during compaction. It does not apply to write-ahead tables or write-ahead partitions.

The value is specified in bytes and must be between ``128`` and ``8160``, inclusive. If unset, PostgreSQL's default
is used, normally ``2040`` bytes with the default block size. The target controls when PostgreSQL attempts to
compress or move large values out of line and the row size it tries to reach; it does not impose a maximum row size.

A lower target can reduce the data read when scanning rows without accessing large attributes, such as JSON
columns. However, reading those attributes may require additional TOAST table access. The best target depends
on the data and query patterns.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.partitions.toast-tuple-target", "512");

After the schema has been created, changes can be made through the :ref:`postgis_cli_update_schema` command,
which also re-writes the partition procedures. If the setting is changed directly in the ``geomesa_userdata``
table, run :ref:`postgis_partition_upgrade` to re-write the procedures. Changes apply to subsequently created
or rebuilt partitions; they do not alter existing partition tables or rows in place.

See the PostgreSQL
`storage parameter documentation <https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS>`__
for more details.

Configuring Data Age-Off
------------------------

Each feature type can be configured to automatically drop partitions older than a certain threshold. This
is accomplished by setting the maximum number of partitions to keep. The age of the data will depend on
the number of hours in each partition (see above). For example, keeping 14 partitions where each partition
is 12 hours will keep the last week's worth of data.

If not specified, data will not be dropped automatically.

Age-off is configured with the key ``pg.partitions.max``.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.partitions.max", "14");

After the schema has been created, changes to the age-off can be made through the
:ref:`postgis_cli_update_schema` command, or by directly updating the ``geomesa_userdata`` table in Postgres.
Changes will take effect within the next 10 minutes.

Configuring Visibility Filtering
--------------------------------

The partitioned PostGIS store supports per-row visibility labels, which can be used to govern which
users can see which records. See :ref:`data_security` for an overview of visibility labels and
authorizations.

.. note::

    Visibility filtering is disabled by default.

When enabled, a hidden ``_vis`` column is added to the partition tables to store each row's visibility label, and the main
view filters rows based on the caller's authorizations. The hidden column is not exposed through the feature type, so it does not
affect the schema seen by clients.

Visibility filtering can be enabled at schema-creation time by setting the key ``pg.vis.enabled`` to
``true``. If not enabled, no ``_vis`` column is added and no filtering is performed.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.vis.enabled", "true");

.. note::

    When visibility filtering is enabled, any rows that do not have a visibility label are assumed to be in error,
    and will throw an exception on write. If a row somehow ends up without a label anyway, it will be hidden from all users.

Visibility filtering cannot be changed after the schema has been created.

.. warning::

    Visibility filtering is configured at the application level. Any user able to query the underlying database directly will
    be able to read all rows.

.. _postgis_filter_world:

Configuring Filter Optimizations
--------------------------------

By default, GeoMesa will ignore filters that contain the entire world, i.e. they encompass all of ``[-180, 180]``
longitude and ``[-90 90]`` latitude. This may speed up such queries, but it may also produce incorrect results if
there are geometries outside the world bounds, or if the data is not stored in ``EPSG:4326``/``WGS84``.

This behavior can be configured through the key ``pg.partitions.filter.world``. The default value is ``false``,
which will ignore whole world filters.

.. code-block:: java

    SimpleFeatureType sft = ....;
     // enable filtering on "whole world" queries
    sft.getUserData().put("pg.partitions.filter.world", "true");

After the schema has been created, changes to the filter optimization can be made through the
:ref:`postgis_cli_update_schema` command, or by directly updating the ``geomesa_userdata`` table in Postgres.
Clients must be restarted in order to pick up the change.

Configuring Tablespaces
-----------------------

Each feature type can be configured to use different tablespaces for the different partition tables. Since
all the writes initially go to the write-ahead table, having it on a fast disk may be beneficial. Conversely,
since the main partitions are written once and not generally updated, having them on slower storage may be
acceptable.

Any configured tablespaces must already exist in the PostreSQL instance being used.

Tablespaces are configured with the keys ``pg.partitions.tablespace.wa``, ``pg.partitions.tablespace.wa-partitions``
and ``pg.partitions.tablespace.main``. See :ref:`pg_partition_table_design` for details on the different tables.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.partitions.tablespace.wa", "fasttablespace");

After the schema has been created, changes to the configured tablespaces can be made through the
:ref:`postgis_cli_update_schema` command, or by directly updating the ``geomesa_userdata`` table in Postgres.
Changes will not be applied to any existing partitions.

Configuring the Maintenance Schedule
------------------------------------

Maintenance scripts are run every 10 minutes to move data between the write-ahead table and the partitioned tables.
By default, the schedule is randomized to avoid all feature types running maintenance at the same time. To specify
the exact minute that the scripts should run, use the key ``pg.partitions.cron.minute``.

The scheduled minute must be between 0 and 8, inclusive. For example, setting the scheduled minute to 1 will
cause the scripts to run at 00:01, 00:11, 00:21, 00:31, etc.

The write-ahead table gets rolled over on the 9th minute of each ten minute block. Thus, running maintenance
at minute 0 will move data out of the write-ahead table the fastest. Since the write-ahead table must be read
for each query, moving data out of it faster may improve performance.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.partitions.cron.minute", "0");

After the schema has been created, changes to the schedule can be made through the :ref:`postgis_cli_update_schema` command.

Configuring WAL logging
-----------------------

PostgreSQL uses a write-ahead log (WAL) to ensure data consistency and durability. By default, the WAL is written
for all changes to the database, including the partitioned tables. Disabling the WAL for the partitioned tables
can significantly improve write performance, but at the cost of data durability. If increased performance is desired,
the WAL can be disabled for the partitioned tables by setting the key ``pg.wal.enabled`` to ``false``.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.wal.enabled", "false");

See the PostgreSQL `documentation <https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-UNLOGGED>`_
for more information on the implications of disabling the WAL.
