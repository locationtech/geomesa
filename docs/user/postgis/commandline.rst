Partitioned PostGIS Command-Line Tools
======================================

The partitioned PostGIS data store is bundled with the ``geomesa-gt`` command-line tools. See :ref:`gt_tools` for
additional details.


Commands
--------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
.. _postgis_partition_upgrade:

=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
.. _postgis_partition_upgrade:

>>>>>>> locationtech-main
=======
<<<<<<< HEAD
.. _postgis_partition_upgrade:

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
.. _postgis_partition_upgrade:

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
>>>>>>> locationtech-main
=======
.. _postgis_partition_upgrade:

>>>>>>> 5c8e27c70f (GEOMESA-3260 Postgis - fix age-off bug (#2958))
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locationtech-main
=======
.. _postgis_partition_upgrade:

=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locationtech-main
``partition-upgrade``
^^^^^^^^^^^^^^^^^^^^^

Update an existing schema to the current GeoMesa version. This will re-write the procedures used to partition
data, and may provide improved functionality or performance. This operation cannot be reverted.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-f, --feature-name *`` The name of the schema
======================== =========================================================
