Installing Partitioned PostGIS
==============================

The partitioned PostGIS data store is bundled with the ``geomesa-gt`` command-line tools. See :ref:`gt_tools` for
installation instructions.

Installing pg_cron in Postgres
------------------------------

The partitioning module requires the ``pg_cron`` PostgreSQL extension to be installed on the database being
used. See `pg_cron <https://github.com/citusdata/pg_cron>`__ for details on installing the extension.

.. _install_pg_partition_geoserver:

Installing Partitioned PostGIS in GeoServer
-------------------------------------------

The regular PostGIS data store can be used in GeoServer, so no additional installation is required.
