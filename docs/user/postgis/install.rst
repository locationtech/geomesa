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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b463955e6d (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> 8f431b9969 (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
<<<<<<< HEAD
=======
=======
>>>>>>> 8f431b9969 (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
>>>>>>> b62213df9e (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
=======
>>>>>>> e54159ef30 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
=======
>>>>>>> 8f431b9969 (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
>>>>>>> 3bcff61f7d (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
=======
>>>>>>> b463955e6d (GEOMESA-3260 Postgis - fix age-off bug (#2958))
.. warning::

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

The partitioned PostGIS GeoServer plugin is bundled by default in a GeoMesa binary distribution. To install, extract
``$GEOMESA_GT_HOME/dist/gs-plugins/geomesa-gt-gs-plugin_${VERSION}-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory.

Restart GeoServer after the JARs are installed.

Note that the regular PostGIS data store can also be used instead, but some queries may be slower due to lack of
optimization.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b62213df9e (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
=======
>>>>>>> 3bcff61f7d (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
=======
>>>>>>> b463955e6d (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
The regular PostGIS data store can be used in GeoServer, so no additional installation is required.
>>>>>>> 5c8e27c70f (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> 8f431b9969 (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b463955e6d (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
=======
The regular PostGIS data store can be used in GeoServer, so no additional installation is required.
>>>>>>> 5c8e27c70f (GEOMESA-3260 Postgis - fix age-off bug (#2958))
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
<<<<<<< HEAD
=======
>>>>>>> b62213df9e (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
=======
=======
The regular PostGIS data store can be used in GeoServer, so no additional installation is required.
>>>>>>> 5c8e27c70f (GEOMESA-3260 Postgis - fix age-off bug (#2958))
>>>>>>> e54159ef30 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> 3bcff61f7d (GEOMESA-3262 Add partitioned PostGIS gs-plugin)
=======
>>>>>>> b463955e6d (GEOMESA-3260 Postgis - fix age-off bug (#2958))

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
