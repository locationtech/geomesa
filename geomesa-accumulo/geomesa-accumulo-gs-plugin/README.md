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
| zookeeper-3.4.6.jar | 792964 |

#### Accumulo 1.6

| jar | size |
| --- | ---- |
| accumulo-core-1.6.6.jar | 4704857 |
| accumulo-fate-1.6.6.jar | 112295 |
| accumulo-trace-1.6.6.jar | 126061 |
| accumulo-server-base-1.6.6.jar | 554489 |
| accumulo-start-1.6.6.jar | 61262 |
| libthrift-0.9.1.jar | 217053 |
| zookeeper-3.4.6.jar | 792964 |
| commons-vfs2-2.0.jar | 415578 |

#### Accumulo 1.7

| jar | size |
| --- | ---- |
| accumulo-core-1.7.2.jar | 4901222 |
| accumulo-fate-1.7.2.jar | 112037 |
| accumulo-trace-1.7.2.jar | 14981 |
| accumulo-server-base-1.7.2.jar | 680093 |
| accumulo-start-1.7.2.jar | 51607 |
| libthrift-0.9.1.jar | 217053 |
| zookeeper-3.4.6.jar | 792964 |
| commons-vfs2-2.1.jar | 441849 |

#### Accumulo 1.8

| jar | size |
| --- | ---- |
| accumulo-core-1.8.0.jar | 5151370 |
| accumulo-fate-1.8.0.jar | 113292 |
| accumulo-trace-1.8.0.jar | 16119 |
| accumulo-server-base-1.8.0.jar | 696285 |
| accumulo-start-1.8.0.jar | 53190 |
| libthrift-0.9.3.jar | 234201 |
| zookeeper-3.4.6.jar | 792964 |
| commons-vfs2-2.1.jar | 441849 |

#### Hadoop 2.2

| jar | size |
| --- | ---- |
| commons-configuration-1.6.jar | 298829 |
| hadoop-auth-2.2.0.jar | 49750 |
| hadoop-client-2.2.0.jar | 2559 |
| hadoop-common-2.2.0.jar | 2735584 |
| hadoop-hdfs-2.2.0.jar | 5242252 |

#### Hadoop 2.4

| jar | size |
| --- | ---- |
| commons-configuration-1.6.jar | 298829 |
| hadoop-auth-2.4.0.jar | 50522 |
| hadoop-client-2.4.0.jar | 2559 |
| hadoop-common-2.4.0.jar | 2850660 |
| hadoop-hdfs-2.4.0.jar | 6825535 |

#### Hadoop 2.5

| jar | size |
| --- | ---- |
| commons-configuration-1.6.jar | 298829 |
| hadoop-auth-2.5.0.jar | 52419 |
| hadoop-client-2.5.0.jar | 2555 |
| hadoop-common-2.5.0.jar | 2963778 |
| hadoop-hdfs-2.5.0.jar | 7095355 |

#### Hadoop 2.6

| jar | size |
| --- | ---- |
| commons-configuration-1.6.jar | 298829 |
| hadoop-auth-2.6.4.jar | 67184 |
| hadoop-client-2.6.4.jar | 2555 |
| hadoop-common-2.6.4.jar | 3321772 |
| hadoop-hdfs-2.6.4.jar | 7919001 |

#### Hadoop 2.7

| jar | size |
| --- | ---- |
| commons-configuration-1.6.jar | 298829 |
| hadoop-auth-2.7.3.jar | 94046 |
| hadoop-client-2.7.3.jar | 26012 |
| hadoop-common-2.7.3.jar | 3473404 |
| hadoop-hdfs-2.7.3.jar | 8278245 |