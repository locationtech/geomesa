GeoMesa GeoServer Plugins
=========================

A straightforward way to render, manipulate, and manage data stored
in GeoMesa data stores is to use `GeoServer <http://www.geoserver.org/>`_,
an open source server for sharing geospatial data. This chapter describes
how to work with the GeoMesa GeoServer plugins.

Instructions for installing the GeoMesa plugins in GeoServer are
available at :ref:`install_geoserver_plugins`.

Go to your GeoServer installation at ``http://<hostname>:8080/geoserver``.
For new installations of GeoServer, the default username is ``admin`` and
password is ``geoserver``, but these may be different at your own installation.

Register a New Accumulo Data Store
----------------------------------

From the main GeoServer page, create a new store by either clicking
"Add stores" in the middle of the **Welcome** page, or anywhere in the
interface by clicking "Data > Stores" in the left-hand menu and then
clicking "Add new Store".

The list of data store types GeoServer knows should be listed. If you
have properly installed the GeoMesa Accumulo GeoServer plugin as described
in :ref:`install_geoserver_plugins`, "Accumulo (GeoMesa)"
should be included in the list under **Vector Data Sources**. If you do not
see this, ensure the plugin is in the right directory and restart GeoServer.

.. image:: _static/img/geoserver-geomesa-accumulo-data-source.png
   :scale: 75%
   :align: center

When you click on "Accumulo (GeoMesa)", several configuration
parameters are available, as listed in the table below:

==================== =======================================================================================
Parameter            Description
==================== =======================================================================================
Workspace *          The GeoServer workspace in which the data store will be placed
Data Source Name *   The name of the data source (should not contain spaces)
Description          A human readable version of the data
instanceId *         The instance ID of the Accumulo installation
zookeepers *         A comma separated list of zookeeper servers (e.g. "zoo1,zoo2,zoo3" or "localhost:2181")
user *               Accumulo username
Password *           Accumulo password
tableName *          The name of the GeoMesa catalog table
auths                Comma-delimited superset of authorizations that will be used for queries via Accumulo.
visibilities         Accumulo visibilities to apply to all written data
queryTimeout         The max time (in sec) a query will be allowed to run before being killed
queryThreads         The number of threads to use per query
recordThreads        The number of threads to use for record retrieval
writeThreads         The number of threads to use for writing records
collectStats         Toggle collection of statistics
caching              Toggle caching of results
==================== =======================================================================================

The required parameters are marked with an asterisk.

Click "Save", and GeoServer will search your Accumulo table for any
GeoMesa-managed feature types.

.. Sections for Kafka, HBase, Bigtable plugins

Publish a GeoMesa Layer
-----------------------

After a GeoMesa data store is successfully created, GeoServer will present a list
of feature types registered in that data store. Click "Publish" next to the
name of a feature type to create a layer of the data in GeoMesa of that type.

You will be taken to the **Edit Layer** screen. To render your layer, you need
to set values for the bounding boxes. In the "Data" pane, enter values for the
bounding boxes. In many cases, you can click on the "Compute from native bounds"
link to computer these values from the data.

.. image:: _static/img/geoserver-layer-bounding-box.png
   :align: center

Click on the "Save" button when you are done.

Preview a Layer
---------------

Click on the "Layer Preview" link in the left-hand menu. Once you see your layer,
click on the "OpenLayers" link, which will open a new tab. If you have ingested
data into GeoMesa, it will be displayed here.

If the data you have ingested is a set of latitude/longitude points, click on
one of the points in the display (rendered by default as red squares), and GeoServer
will report detailed records stored in the GeoMesa store in the region underneath
the map area.

Click on the "Toggle options toolbar" icon in the upper-left corner
of the preview window. The right-hand side of the screen will include
a "Filter" text box. Enter a search query on the attributes of the feature type
of the data you have ingested, and press on the "play" icon. The display will now
show only those points matching your filter criterion.

This is a CQL filter, which can be constructed in various ways to query data. You can
find more information about CQL from `GeoServer's CQL
tutorial <http://docs.geoserver.org/latest/en/user/tutorials/cql/cql_tutorial.html>`__.

Analysis with WPS
-----------------

As described by the Open Geospatial Consortium's page on WPS, 

    The OpenGIS® Web Map Service Interface Standard (WMS) provides a simple HTTP
    interface for requesting geo-registered map images from one or more
    distributed geospatial databases. A WMS request defines the geographic
    layer(s) and area of interest to be processed. The response to the request is
    one or more geo-registered map images (returned as JPEG, PNG, etc) that can be
    displayed in a browser application. The interface also supports the ability to
    specify whether the returned images should be transparent so that layers from
    multiple servers can be combined or not.
 
A tool like GeoServer (once its WPS plugin has been installed) uses WPS to
retrieve data from GeoMesa. WPS processes can be chained, letting you use
additional WPS requests to build on the results of earlier ones.

