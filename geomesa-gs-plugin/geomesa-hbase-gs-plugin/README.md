# GeoMesa HBase GeoServer Plugin

The HBase GeoServer plugin is a shaded jar that contains HBase 1.1.5. To change HBase versions,
you would need to update the pom and re-build this module.

### Build Instructions

To build this module, use the `hbase` Maven profile:

```bash
$ mvn clean install -Phbase
```

### Installation Instructions

After building, extract `target/geomesa-hbase-gs-plugin-<version>-install.tar.gz` into GeoServer's
WEB-INF/lib directory.

### Additional Resources

The HBase data store requires the configuration file `hbase-site.xml` to be on the classpath. This can
be accomplished by placing the file in `geoserver/WEB-INF/classes` (you should make the directory if it
doesn't exist).
