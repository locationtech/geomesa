# GeoMesa HBase/Bigtable

HBase/Bigtable support is broken out into these modules:

## HBase

### [HBase Data Store](geomesa-hbase-datastore)

The HBase data store module contains the code for using an HBase-backed GeoTools data store.

### [HBase GeoServer Plugin](../geomesa-gs-plugin/geomesa-hbase-gs-plugin)

The GeoServer plugin module builds a bundle containing all of the JARs necessary to use the
HBase data store in GeoServer.

## Bigtable

### [Bigtable Data Store](geomesa-bigtable-datastore)

This module contains a stub POM for building a [Google Cloud Bigtable](https://cloud.google.com/bigtable)-backed GeoTools
data store.

### [Bigtable GeoServer Plugin](../geomesa-gs-plugin/geomesa-bigtable-gs-plugin)

The plugin module contains a stub POM for building a bundle containing
all of the JARs to use the [Google Cloud Bigtable](https://cloud.google.com/bigtable) data store in GeoServer.
