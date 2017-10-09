Using the Bigtable Data Store in GeoServer
==========================================

.. note::

    See :doc:`/user/geoserver` for general information for working with GeoMesa in Geoserver.

From the main GeoServer page, create a new store by either clicking
"Add stores" in the middle of the **Welcome** page, or anywhere in the
interface by clicking "Data > Stores" in the left-hand menu and then
clicking "Add new Store".

If you have properly installed the GeoMesa Bigtable GeoServer plugin as described
in :ref:`install_bigtable_geoserver`, "Bigtable (GeoMesa)" should be included in the list
under **Vector Data Sources**. If you do not see this, check that you unpacked the
plugin JARs into in the right directory and restart GeoServer.

On the "Add Store" page, select "Google Bigtable (GeoMesa)", and fill out the
parameters. The parameters are described in :ref:`bigtable_parameters`.

Other configuration information is taken from ``hbase-site.xml`` (see :ref:`install_bigtable_geoserver`).

Click "Save", and GeoServer will search Bigtable for any GeoMesa-managed feature types.
