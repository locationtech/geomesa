Partitioned PostGIS Command-Line Tools
======================================

The partitioned PostGIS data store is bundled with the ``geomesa-gt`` command-line tools. See :ref:`gt_tools` for
additional details.

Commands
--------

.. _postgis_partition_upgrade:

``partition-upgrade``
^^^^^^^^^^^^^^^^^^^^^

Update an existing schema to the current GeoMesa version. This will re-write the procedures used to partition
data, and may provide improved functionality or performance. This operation cannot be reverted.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-f, --feature-name *`` The name of the schema
======================== =========================================================

.. _postgis_cli_update_schema:

``update-schema``
^^^^^^^^^^^^^^^^^

Alter an existing ``SimpleFeatureType``. For PostGIS, this command can only be used to modify configuration
values. See :ref:`postgis_index_config` for available configuration values.

This command will also re-write the partition procedures as necessary to apply the configuration changes.

======================== ==============================================================
Argument                 Description
======================== ==============================================================
``-f, --feature-name *`` The name of the schema to operate on
``--add-user-data``      Add or update an entry in the feature type user data
======================== ==============================================================

The ``--add-user-data`` parameter can be used to add or update any user data key. See :ref:`postgis_index_config` for
some examples of configurable values. Entries can be specified as ``<key>:<value>``.
