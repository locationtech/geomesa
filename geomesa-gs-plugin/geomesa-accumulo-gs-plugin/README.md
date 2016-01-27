# GeoMesa Accumulo GeoServer 2.8.x Plugin

### Installation Instructions

After building, extract `target/geomesa-accumulo-gs-plugin-<version>-install.tar.gz` into GeoServer's
WEB-INF/lib directory.

### Additional Jars

You will also need to copy the required Accumulo and Hadoop jars. The exact jars will vary
depending on your environment.

If you have the GeoMesa tools installed, you can automatically download the appropriate jars using
`$GEOMESA_HOME/bin/install-hadoop-accumulo.sh`.

```bash
> $GEOMESA_HOME/bin/install-hadoop-accumulo.sh $CATALINA_HOME/webapps/geoserver/WEB-INF/lib/
```
Note: the script will download the jars specified in the GeoMesa pom when the tools were built.

See below for some common configurations:

#### Accumulo 1.5

| jar | size |
| --- | ---- |
| accumulo-core-1.5.3.jar | 3748223 |
| accumulo-fate-1.5.3.jar | 99254 |
| accumulo-start-1.5.3.jar | 53912 |
| accumulo-trace-1.5.3.jar | 116912 |
| libthrift-0.9.0.jar | 347531 |
| zookeeper-3.3.6.jar | 608239 |

#### Accumulo 1.6

| jar | size |
| --- | ---- |
| accumulo-core-1.6.4.jar | 4649190 |
| accumulo-fate-1.6.4.jar | 108830 |
| accumulo-server-base-1.6.4.jar | 547323 |
| accumulo-trace-1.6.4.jar | 124572 |
| libthrift-0.9.1.jar | 217053 |
| zookeeper-3.4.5.jar | 1315806 |


#### Hadoop 2.2

| jar | size |
| --- | ---- |
| commons-configuration-1.6.jar | 298829 |
| hadoop-auth-2.2.0.jar | 49750 |
| hadoop-common-2.2.0.jar | 2735584 |
| hadoop-hdfs-2.2.0.jar | 5242252 |

#### Hadoop 2.4

| jar | size |
| --- | ---- |
| commons-configuration-1.6.jar | 298829 |
| hadoop-auth-2.4.0.jar | 50522 |
| hadoop-common-2.4.0.jar | 2850660 |
| hadoop-hdfs-2.4.0.jar | 6825535 |
