# GeoMesa GeoServer 2.9.x Plugins

GeoMesa provides an easy way to integrate with GeoServer 2.9.x. When built, the various GeoServer
modules will all produce a tar.gz file containing all the artifacts needed for GeoServer.

Note: if you are interested in support for older versions of GeoServer, the GeoMesa 1.2.x series works with GeoServer 2.8.x.  For GeoServer 2.5.x, see [geomesa-1.1.0-rc.7](tree/geomesa-1.1.0-rc.7).

### Installation

To install, simply extract `<module>/target/geomesa-<mdodule>-gs-plugin-<version>-install.tar.gz`
into GeoServer's WEB-INF/lib directory.

### Environment Specific Jars

Because GeoMesa works with multiple versions of Hadoop, Accumulo, Kafka, etc, we do not bundle those jars
in our install. The appropriate jars need to be installed separately. For details, see each individual
plugin module.
