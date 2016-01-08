# GeoMesa Accumulo Distributed Runtime

The distributed runtime for Accumulo produces a jar that must be installed in the `$ACCUMULO_HOME/lib/ext` folder
on each of the tablet servers of the Accumulo cluster. It contains Accumulo iterators and GeoMesa code required to 
query the GeoMesa datastore.

When upgrading versions of GeoMesa it is necessary to update the distributed runtime jar and keep it in sync with any
geomesa datastore plugins that you are using (e.g. GeoServer, Kafka, etc.)