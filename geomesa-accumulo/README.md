# GeoMesa Accumulo

Accumulo support is broken out into three modules:

### [Data Store](geomesa-accumulo-datastore)

The data store module contains all the code for using an Accumulo-backed GeoTools data store.

### [Distributed Runtime](geomesa-accumulo-distributed-runtime)

The distributed runtime module builds the server-side jar that is installed in Accumulo for distributed processing.

### [GeoServer Plugin](../geomesa-gs-plugin/geomesa-accumulo-gs-plugin)

The GeoServer plugin module builds the files necessary to use the Accumulo data store in GeoServer.