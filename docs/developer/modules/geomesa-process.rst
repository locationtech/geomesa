geomesa-process
===============

The following analytic processes are available and optimized on GeoMesa
data stores.

-  ``DensityProcess`` - computes a density heatmap for a CQL query
-  ``TemporalDensityProcess`` - returns a time series for a CQL query
-  ``TubeSelectProcess`` - performs a correlated search across
   time/space dimensions
-  ``ProximitySearchProcess`` - performs a nearest neighbor search
-  ``QueryProcess`` - optimizes GeoMesa queries in WPS chains
-  ``KNearestNeighborSearchProcess`` - performs a KNN search
-  ``UniqueProcess`` - identifies unique values for an attribute in
   results of a CQL query

Installation
------------

To install the GeoMesa processes, first deploy the main GeoMesa
GeoServer plugin in ``${GEOSERVER_HOME}/WEB-INF/lib``. Then copy the
``geomesa-process-accumulo1.5-${VERSION}.jar`` into the same directory.
