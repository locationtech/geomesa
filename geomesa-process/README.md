# GeoMesa Processes

The following analytic processes are available and optimized on GeoMesa data stores.

* `DensityProcess` - computes a density heatmap for a CQL query
*  HashAttributeColorProcess and HashAttributeProcess - computes an additional 'hash' attribute which is useful for styling.
* `KNearestNeighborSearchProcess` - performs a KNN search
*  Point2PointProcess - aggregates a collection of points into a collection of line segments
* `ProximitySearchProcess` - performs a nearest neighbor search
*  SamplingProcess - uses statistical sampling to reduces the features returned by a query
* `StatsIteratorProcess` - returns various stats for a CQL query
* `TubeSelectProcess` - performs a correlated search across time/space dimensions
* `QueryProcess` - optimizes GeoMesa queries in WPS chains
* `UniqueProcess` - identifies unique values for an attribute in results of a CQL query
* `JoinProcess` - returns merged features from two different schemas using a common attribute field


## Installation

The above extensions are particular to the Accumulo datastore.

While they can be used independently, the common use case is to use them with GeoServer.  To deploy them in GeoServer, one will require a) the GeoMesa Accumulo datastore plugin, b) the GeoServer WPS extension, and c) the `geomesa-process-${VERSION}.jar` to be deployed in `${GEOSERVER_HOME}/WEB-INF/lib`.  

The GeoMesa Accumulo datastore plugin and GeoMesa process jars are both available in the binary distribution in the gs-plugins directory.

Documentation about the GeoServer WPS Extension (including download instructions) is available here: http://docs.geoserver.org/2.9.1/user/services/wps/install.html.

To verify the install, start GeoServer, and you should see a line like `INFO [geoserver.wps] - Found 11 bindable processes in GeoMesa Process Factory`.

In the GeoServer web UI, click 'Demos' and then 'WPS request builder'.  From the request builder, under 'Choose Process', click on any of the 'geomesa:' options to build up example requests and in some cases see results.
