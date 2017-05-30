# GeoMesa Processes

The following analytic processes are available and optimized on GeoMesa data stores.

* `DensityProcess` - computes a density heatmap for a CQL query
*  HashAttributeColorProcess and HashAttributeProcess - computes an additional 'hash' attribute which is useful for styling.
* `KNearestNeighborSearchProcess` - performs a KNN search
*  Point2PointProcess - aggregates a collection of points into a collection of line segments
* `ProximitySearchProcess` - performs a nearest neighbor search
*  SamplingProcess - uses statistical sampling to reduces the features returned by a query
* `StatsProcess` - returns various stats for a CQL query
* `TubeSelectProcess` - performs a correlated search across time/space dimensions
* `QueryProcess` - optimizes GeoMesa queries in WPS chains
* `UniqueProcess` - identifies unique values for an attribute in results of a CQL query
* `JoinProcess` - returns merged features from two different schemas using a common attribute field


## Installation

Most processes should work with any datastore, however some datastores have optimized distributed processing.

While they can be used independently, the common use case is to use them with GeoServer.  To deploy them in GeoServer requires:
 * a GeoMesa datastore plugin
 * the GeoServer WPS extension
 * the `geomesa-process-wps_2.11-${VERSION}.jar` to be deployed in `${GEOSERVER_HOME}/WEB-INF/lib`.

The datastore plugins and GeoMesa process jars are both available in the relevant binary distribution in the gs-plugins directory.

Documentation about the GeoServer WPS Extension (including download instructions) is available here: http://docs.geoserver.org/2.9.1/user/services/wps/install.html.

To verify the install, start GeoServer, and you should see a line like `INFO [geoserver.wps] - Found 15 bindable processes in GeoMesa Process Factory`.

In the GeoServer web UI, click 'Demos' and then 'WPS request builder'.  From the request builder, under
'Choose Process', click on any of the 'geomesa:' options to build up example requests and in some cases see results.
