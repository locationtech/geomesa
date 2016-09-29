# GeoMesa HBase GeoServer Plugin

The HBase GeoServer plugin is a shaded JAR that contains HBase 1.1.5. To change HBase versions,
you would need to update `pom.xml` and rebuild this module.

### Build Instructions

To build this module, use the `hbase` Maven profile:

```bash
$ mvn clean install -Phbase
```

### Installation Instructions

After building, extract `target/geomesa-hbase-gs-plugin-<version>-install.tar.gz` into GeoServer's ``WEB-INF/lib`` directory.

This distribution does not include the Hadoop or Zookeeper JARs; the following JARs
should be copied from the ``lib`` directory of your HBase or Hadoop installations into
GeoServer's ``WEB-INF/lib`` directory:

 * hadoop-annotations-2.5.1.jar
 * hadoop-auth-2.5.1.jar
 * hadoop-common-2.5.1.jar
 * hadoop-mapreduce-client-core-2.5.1.jar
 * hadoop-yarn-api-2.5.1.jar
 * hadoop-yarn-common-2.5.1.jar
 * zookeeper-3.4.6.jar
 * commons-configuration-1.6.jar

(Note the versions may vary depending on your installation.)

### Additional Resources

The HBase data store requires the configuration file `hbase-site.xml` to be on the classpath. This can
be accomplished by placing the file in `geoserver/WEB-INF/classes` (you should make the directory if it
doesn't exist).
