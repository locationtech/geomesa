# GeoMesa HBase/BigTable

HBase/BigTable support is broken out into these modules:

## HBase

### [HBase Data Store](geomesa-hbase-datastore)

The HBase data store module contains the code for using an HBase-backed GeoTools data store.

### [HBase GeoServer Plugin](../geomesa-gs-plugin/geomesa-hbase-gs-plugin)

The GeoServer plugin module builds a bundle containing all of the JARs necessary to use the
HBase data store in GeoServer.

## BigTable

### [BigTable Data Store](geomesa-bigtable-datastore)

This module contains a stub POM for building a Google BigTable-backed GeoTools
data store.

### [BigTable GeoServer Plugin](../geomesa-gs-plugin/geomesa-bigtable-gs-plugin)

The plugin module contains a stub POM for building a bundle containing
all of the JARs to use the Google BigTable data store in GeoServer.