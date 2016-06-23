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
| accumulo-core-1.5.4.jar | 3749121 |
| accumulo-fate-1.5.4.jar | 99288 |
| accumulo-start-1.5.4.jar | 53943 |
| accumulo-trace-1.5.4.jar | 116943 |
| libthrift-0.9.0.jar | 347531 |
| zookeeper-3.3.6.jar | 608239 |

#### Accumulo 1.6

| jar | size |
| --- | ---- |
| accumulo-core-1.6.5.jar | 4654993 |
| accumulo-fate-1.6.5.jar | 111114 |
| accumulo-trace-1.6.5.jar | 124575 |
| accumulo-server-base-1.6.5.jar | 550180 |
| accumulo-start-1.6.5.jar | 60719 |
| libthrift-0.9.1.jar | 217053 |
| zookeeper-3.4.6.jar | 792964 |
| commons-vfs2-2.0.jar | 415578 |


#### Hadoop 2.2

| jar | size |
| --- | ---- |
| commons-configuration-1.6.jar | 298829 |
| hadoop-auth-2.2.0.jar | 49750 |
| hadoop-client-2.2.0.jar | 2559 |
| hadoop-common-2.2.0.jar | 2735584 |
| hadoop-hdfs-2.2.0.jar | 5242252 |

#### Hadoop 2.4-2.7 (adjust as needed)

| jar | size |
| --- | ---- |
| commons-configuration-1.6.jar | 298829 |
| hadoop-auth-2.6.4.jar | 67190 |
| hadoop-client-2.6.4.jar | 2555 |
| hadoop-common-2.6.4.jar | 3322114 |
| hadoop-hdfs-2.6.4.jar | 7919364 |

