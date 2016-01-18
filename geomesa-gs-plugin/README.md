# GeoMesa GeoServer Plugins

GeoMesa provides an easy way to integrate with GeoServer. When built, the various GeoServer modules will all
produce a zip file containing all the artifacts needed for GeoServer.

### Installation

To install, simply unzip `<module>/target/geomesa-<mdodule>-gs-plugin-<version>-install.zip`
into GeoServer's WEB-INF/lib directory.

### Environment Specific Jars

Because GeoMesa works with multiple versions of Hadoop, Accumulo, Kafka, etc, we do not bundle those jars
in our install. The appropriate jars need to be installed separately. For details, see each individual
plugin module.
