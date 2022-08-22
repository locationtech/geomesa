Partitioned PostGIS Command-Line Tools
======================================

The partitioned PostGIS data store is bundled with the ``geomesa-gt`` command-line tools. See :ref:`gt_tools` for
additional details.


Commands
--------

``partition-upgrade``
^^^^^^^^^^^^^^^^^^^^^

Update an existing schema to the current GeoMesa version. This will re-write the procedures used to partition
data, and may provide improved functionality or performance. This operation cannot be reverted.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-f, --feature-name *`` The name of the schema
======================== =========================================================
