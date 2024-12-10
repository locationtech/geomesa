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
:ref:`postgis_cli_update_schema` command. Changes will not be applied to any existing partitions. If the partition
size is **increased**, any recent partitions that would overlap with the new partition size will need to be
manually dropped and the data re-inserted in the write-ahead table in order to prevent partition range conflict errors.

Configuring Index Resolution
----------------------------

Each feature type can be configured with a number of pages per range. The partition tables use a
`BRIN <https://www.postgresql.org/docs/current/brin-intro.html>`__ index, which is a lossy index structure.
The number of data pages stored in each index range controls how lossy, and how large the index becomes.
By default, Postgres stores 128 pages in each range. Storing fewer pages will generally make the index more
efficient, at the cost of requiring more space; however, the optimal number will depend on data characteristics
and typical query patterns.

The number of pages is configured with the key ``pg.partitions.pages-per-range``.

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("pg.partitions.pages-per-range", "64");

The index resolution cannot be changed after the schema has been created.

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
:ref:`postgis_cli_update_schema` command, or by directly updating the ``partition_tablespaces`` table in Postgres.
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

After the schema has been created, changes to the schedule can be made through the
:ref:`postgis_cli_update_schema` command.

Configuring WAL logging
-----------------------

PostgreSQL uses a write-ahead log (WAL) to ensure data consistency and durability. By default, the WAL is written
for all changes to the database, including the partitioned tables. Disabling the WAL for the partitioned tables
can significantly improve write performance, but at the cost of data durability. If increased performance is desired,
the WAL can be disabled for the partitioned tables by setting the key ``pg.wal.enabled`` to ``false``.

See the PostgreSQL `documentation <https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-UNLOGGED>`_
 for more information on the implications of disabling the WAL.