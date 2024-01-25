Design Overview
===============

Motivation
----------

PostGIS is a widely used spatial database that is easily deployed, however it is limited in the total number of
records it can handle in a single table. This module makes it possible to store much larger data sets using
PostgreSQL's native partitioning, by automatically sorting data into time-based partitions.

Overview
--------

The PostGIS partitioning module is an extension of the GeoTools
`JDBC data store <https://docs.geotools.org/stable/userguide/library/jdbc/datastore.html>`__, implemented
as a custom dialect. It is designed for sensor data, where each record has a timestamp and records are
mostly ingested soon after they are generated.

As data is written, it gets sorted into partitions based on record timestamps. Recent data gets sorted
into smaller partitions, then into larger partitions once it has reached a certain age. The partitions are
sorted spatio-temporally, to reduce page reads during typical queries. Since the data is sorted, it can be
effectively indexed using a spatial ``BRIN`` index, which is much smaller than a standard ``GIST`` index. During
queries, PostgreSQL will automatically skip over partitions that don't match the query, using standard partition
pruning. This allows massive data sets to be efficiently queried with temporal predicates, although non-temporal
queries may not perform well.

.. _pg_partition_table_design:

Table Design
------------

The data is split into one primary view and four tables, names of which are prefixed by the feature type:

  * main view (named after the feature type) - a ``UNION ALL`` of the other tables, this is the view
    that should generally be used for all external reads and writes.

  * ``_wa`` - the write-ahead table. All writes to the main view are relayed to this table using a trigger. The
    table is partitioned using table inheritance (as there may be overlap between the partitions), and always has one
    "active" partition that's receiving all writes. Records will continue accumulating in this partition for a period
    of 10 minutes, after which a new partition will be created and marked "active" (meaning that records will now be
    written to this new partition). All of the records in the older partitions will be transferred over to the
    ``_wa_partition`` table (in spatially sorted order) and partitioned into 10-minute increments in that table.

  * ``_wa_partition`` - contains the data transferred over from the ``_wa`` table, after having been spatially sorted
    and partitioned into 10-minute increments using declarative partitioning. This table is designed to handle
    time-latent data, and to store enough data to fill an entire main partition, while still providing partition
    pruning for queries. Once enough time elapses, data from this table gets transferred over to the ``_partition``
    table (in spatially sorted order) and partitioned based on the value of the key ``pg.partitions.interval.hours``.

  * ``_partition`` - contains the data transferred over from the ``_wa_partition`` table, after having been spatially
    sorted and partitioned using declarative time partitioning. This table uses a BRIN index, which is small but
    performs well on sorted data. Keeping the data sorted also reduces the number of page hits required for most spatial
    queries.

  * ``_spill`` - this table contains any data that gets written after the ``_wa_partition`` table's data is transferred
    over to the ``_partition`` table and spatially sorted and partitioned there. A separate table is maintained for
    such data, because the ``_partition`` table would no longer be sorted if this data was written there. 

Helper Tables
-------------

In addition to the ones above, the following additional tables are used:

  * ``_analyze_queue`` - tracks partitions which have been modified. Modified partitions will have ``ANALYZE``
    invoked on them in a separate process.
  * ``_sort_queue`` - tracks out-of-order inserts into the main partition table. This can be used to diagnose
    slow queries, as unsorted data can negatively impact the effectiveness of the ``BRIN`` index.

Maintenance Scripts
-------------------

Several PLPG/SQL procedures are used to move data around according to the design described above. The ``pg_cron``
extension is used to schedule these tasks. The script names are prefixed by the feature type, and created based
on the feature type configuration (i.e. field names, partition size, etc). The procedures consist of:

  * ``_roll_wa`` - creates a new write-ahead table every 10 minutes
  * ``_partition_maintenance`` umbrella function for invoking all the partition scripts at once
  * ``_partition_wa`` - moves data from old write ahead tables into the ``_wa_partition`` table
  * ``_merge_wa_partitions`` - moves data from the ``_wa_partition`` table into the main ``_partition`` table
  * ``_age_off`` (if configured) - drops any partitions that have exceeded the maximum amount of data to keep
  * ``_analyze_partitions`` - runs ``ANALYZE`` on any partitions that have been updated
  * ``_partition_sort`` - manually called to re-sort a partition
