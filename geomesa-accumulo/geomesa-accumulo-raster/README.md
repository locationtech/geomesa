# GeoMesa Raster

### About

The Raster support in GeoMesa is currently limited in scope and has a few important caveats that are
noted here.

The Raster support was intended to contain image pyramids that are EPSG:4326 with tiles on the order
of Kb in size. Any rasters attempted to be ingested will need to be tiled prior to ingest, and the 
tiles must have disjoint extents. Currently only single band images are supported.
Sharding is not currently used to spread the table across the cluster. 

### Ingest

Currently ingest is accomplished by using the `ingest-raster` command in the GeoMesa command line tools.
See the tools README for usage.

### GeoServer

The GeoMesa-GeoServer plugin contains the GeoMesaCoverageFormat which is used via the GeoServer UI
to create a store for GeoMesa Raster table. Once a layer is published the data can be queried via WMS and WCS 1.0.0. 
Currently the Plugin is biased to grab tiles at a finer spatial resolution than the wms query window, 
and will down-sample the mosaic. 

### Building Instructions

If you wish to build this project separately, you can with mvn clean install

```geomesa/geomesa-raster> mvn clean install```

This will produce a jar in target/