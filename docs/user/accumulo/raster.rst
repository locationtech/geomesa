.. _accumulo_raster:

GeoMesa Raster
==============

The Raster support in GeoMesa (in the ``geomesa-accumulo/geomesa-accumulo-raster`` module)
is currently limited in scope and has a few important caveats that
are noted here.

The Raster support was intended to contain image pyramids that are
EPSG:4326 with tiles on the order of Kb in size. Any rasters attempted
to be ingested will need to be tiled prior to ingest, and the tiles must
have disjoint extents. Currently only single band images are supported.
Sharding is not currently used to spread the table across the cluster.

Installation
------------

Raster support requires that the raster-specific Accumulo runtime jar be installed
in Accumulo. Follow the instructions at :ref:`install_accumulo_runtime`, but be sure to
use the ``geomesa-accumulo-raster-distributed-runtime`` JAR instead of the standard
``geomesa-accumulo-distributed-runtime`` JAR.

Ingest
------

Currently ingest is accomplished by using the ``ingest-raster`` command
in the GeoMesa Accumulo command line tools. See :ref:`accumulo_tools_raster` for more
information.

GeoServer
---------

Raster integration with GeoServer requires that the standard GeoMesa Accumulo plugin is installed,
as described in :ref:`install_accumulo_geoserver`. In addition, GeoMesa provides a GeoServer
community module that must be installed separately. The community module is available on `Github`__ -
follow the instructions there to build and install it.

__ https://github.com/ccri/geoserver/tree/geomesa_community_2.8.x_1.2/src/community/geomesa

.. warning::

    The GeoMesa community module is not being actively maintained, and may require code changes
    to work with current GeoMesa/GeoServer releases.

Once installed, you can add a raster store with the "Accumulo (Geomesa Raster)" coverage store
in the GeoServer UI. Once a layer is published the data can be queried via WMS and WCS 1.0.0.
Currently the Plugin is biased to grab tiles at a finer spatial resolution than the wms query window,
and will down-sample the mosaic.

Building Instructions
---------------------

If you wish to build this project separately, you can with maven:

::

    geomesa/geomesa-accumulo/geomesa-accumulo-raster$ mvn clean install

This will produce a JAR in ``target``.
