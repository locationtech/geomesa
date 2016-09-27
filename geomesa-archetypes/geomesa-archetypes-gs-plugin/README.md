# GeoMesa GeoServer 2.8.x Plugins

GeoMesa provides an easy way to integrate with GeoServer 2.8.x. When built, the various GeoServer
modules will all produce a tar.gz file containing all the artifacts needed for GeoServer.

Note: if you can't use GeoServer 2.8.x, older versions of GeoMesa are compatible with GeoServer 2.5.x.
See [geomesa-1.1.0-rc.7](tree/geomesa-1.1.0-rc.7) for the last compatible version.

### Installation

To install, simply extract `<module>/target/geomesa-<mdodule>-gs-plugin-<version>-install.tar.gz`
into GeoServer's WEB-INF/lib directory.

### Environment Specific Jars

Because GeoMesa works with multiple versions of Hadoop, Accumulo, Kafka, etc, we do not bundle those jars
in our install. The appropriate jars need to be installed separately. For details, see each individual
plugin module.
