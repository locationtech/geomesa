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

Upgrading Existing Schemas
--------------------------

Any existing feature types will not automatically benefit from upgrading the GeoMesa version, as the functions
and procedures are stored in PostGIS itself. After upgrading GeoMesa versions, the procedures can be upgraded
in one of two ways:

Command-line Tools Upgrade
^^^^^^^^^^^^^^^^^^^^^^^^^^

The GeoMesa command line tools come with a command to upgrade the schema. See :ref:`postgis_partition_upgrade` for
details.

Drop and Re-create the Main View
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Alternatively, the main view for a feature type (which has the same name as the feature type) can be dropped
using ``psql`` or another tool::

    DROP VIEW my_feature_type;

Then the feature type can then be re-created using ``createSchema``. Existing data will be preserved in the
partitioned tables.
