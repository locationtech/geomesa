Using the FileSystem Data Store in GeoServer
============================================

.. note::

    For general information on working with GeoMesa GeoServer plugins,
    see :doc:`/user/geoserver`.

From the main GeoServer page, create a new store by either clicking
"Add stores" in the middle of the **Welcome** page, or anywhere in the
interface by clicking "Data > Stores" in the left-hand menu and then
clicking "Add new Store".

If you have properly installed the GeoMesa FileSystem GeoServer plugin as described in
:ref:`install_fsds_geoserver`, "FileSystem (GeoMesa)" should be included in the list
under **Vector Data Sources**. If you do not see this, check that you unpacked the
plugin JARs into in the right directory and restart GeoServer.

On the "Add Store" page, select "FileSystem (GeoMesa)". The parameters are described in :ref:`fsds_parameters`.

Click "Save", and GeoServer will search the filesystem path for any GeoMesa-managed feature types.
