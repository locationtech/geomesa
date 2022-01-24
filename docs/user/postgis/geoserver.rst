Using the Partitioned PostGIS Data Store in GeoServer
=====================================================

.. note::

    For general information on working with GeoMesa GeoServer plugins,
    see :doc:`/user/geoserver`.

From the main GeoServer page, create a new store by either clicking
"Add stores" in the middle of the **Welcome** page, or anywhere in the
interface by clicking "Data > Stores" in the left-hand menu and then
clicking "Add new Store".

On the "Add Store" page, select "PostGIS", and fill out the
parameters. The parameters are described in :ref:`pg_partition_parameters`.

Click "Save", and GeoServer will search PostGIS for any feature types.
Select the primary view that is named after your particular schema, and not any of the
partitioned sub-tables, which may also show up.
