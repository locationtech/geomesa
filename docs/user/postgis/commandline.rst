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
<<<<<<< HEAD
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
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e54159ef30 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b463955e6d (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
.. _postgis_partition_upgrade:

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> e54159ef30 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
>>>>>>> b463955e6d (GEOMESA-3260 Postgis - fix age-off bug (#2958))
>>>>>>> locatelli-main
=======
.. _postgis_partition_upgrade:

>>>>>>> 5c8e27c70f (GEOMESA-3260 Postgis - fix age-off bug (#2958))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b463955e6d (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
.. _postgis_partition_upgrade:

=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9bd39d5310 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e54159ef30 (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b463955e6d (GEOMESA-3260 Postgis - fix age-off bug (#2958))
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
``partition-upgrade``
^^^^^^^^^^^^^^^^^^^^^

Update an existing schema to the current GeoMesa version. This will re-write the procedures used to partition
data, and may provide improved functionality or performance. This operation cannot be reverted.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-f, --feature-name *`` The name of the schema
======================== =========================================================
