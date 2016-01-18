# GeoMesa Accumulo GeoServer Plugin

### Installation Instructions

After building, unzip `target/geomesa-accumulo-gs-plugin-<version>-install.zip` into GeoServer's
WEB-INF/lib directory.

### Additional Jars

You will also need to copy the required Accumulo and Hadoop jars. The exact jars will vary
depending on your environment. Some common configurations:

#### Accumulo 1.5

| jar | size |
| --- | ---- |
| accumulo-core-1.5.2.jar | 3748459 |
| accumulo-fate-1.5.2.jar | 99782 |
| accumulo-start-1.5.2.jar | 53902 |
| accumulo-trace-1.5.2.jar | 116904 |
| commons-configuration-1.6.jar | 298829 |
| libthrift-0.9.0.jar | 347531 |
| zookeeper-3.3.6.jar | 608239 |

#### Hadoop 2.2

| jar | size |
| --- | ---- |
| hadoop-annotations-2.2.0.jar | 16778 |
| hadoop-auth-2.2.0.jar | 49750 |
| hadoop-client-2.2.0.jar | 2559 |
| hadoop-common-2.2.0.jar | 2735584 |
| hadoop-hdfs-2.2.0.jar | 5242252 |
