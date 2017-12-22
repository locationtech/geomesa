# GeoMesa Bigtable GeoServer Plugin

The Bigtable GeoServer plugin provides Google Cloud Bigtable support for GeoServer.

### Build Instructions

To build this module, use the `bigtable` Maven profile:

```bash
$ mvn clean install -Pbigtable
```

### Installation Instructions

After building, extract `target/geomesa-bigtable-gs-plugin-<version>-install.tar.gz` into GeoServer's ``WEB-INF/lib`` directory.

This distribution does not include the Hadoop or HBase JARs; the following JARs
should be copied into GeoServer's ``WEB-INF/lib`` directory:

 * commons-configuration-1.6.jar
 * hadoop-annotations-2.4.1.jar
 * hadoop-auth-2.5.1.jar
 * hadoop-client-2.5.2.jar
 * hadoop-common-2.5.2.jar
 * hadoop-hdfs-2.5.1.jar
 * hbase-annotations-1.1.2.jar
 * hbase-client-1.2.3.jar
 * hbase-common-1.2.3.jar
 * hbase-hadoop2-compat-1.1.2.jar
 * hbase-hadoop-compat-1.1.2.jar
 * hbase-prefix-tree-1.1.2.jar
 * hbase-procedure-1.1.2.jar
 * hbase-protocol-1.2.3.jar
 * hbase-server-1.1.2.jar
 * protobuf-java-2.5.0.jar

(Note the versions may vary depending on your installation.)

### Additional Resources

The Bigtable data store requires the configuration file `hbase-site.xml` to be on the classpath. This can
be accomplished by placing the file in `geoserver/WEB-INF/classes` (you should make the directory if it
doesn't exist).
