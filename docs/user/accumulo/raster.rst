.. _accumulo_raster:

GeoMesa Raster
==============

.. warning::

  GeoMesa raster support is deprecated and will be removed in a future version.

The Raster support in GeoMesa (the ``geomesa-accumulo/geomesa-accumulo-raster`` module)
is currently limited in scope and has a few important caveats that are noted here.

The Raster support was intended to contain image pyramids that are
``EPSG:4326`` with tiles on the order of Kb in size. Any rasters will need to
be tiled prior to ingest, and the tiles must have disjoint extents.

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
as described in :ref:`install_accumulo_geoserver`. In addition, the ``geomesa-gs-raster`` JAR
must be installed from the `GeoMesa GeoServer`__ project. It may be built from source, or is
available from Maven central::

    <dependency>
      <groupId>org.geomesa.geoserver</groupId>
      <artifactId>geomesa-gs-raster</artifactId>
    </dependency>

__ https://github.com/geomesa/geomesa-geoserver

Once installed, you can add a raster store with the "Accumulo (Geomesa Raster)" coverage store
in the GeoServer UI. Once a layer is published the data can be queried via WMS and WCS 1.0.0.
Currently the Plugin is biased to grab tiles at a finer spatial resolution than the wms query window,
and will down-sample the mosaic.
