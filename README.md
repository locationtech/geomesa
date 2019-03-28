<p align="center">
  <a href="http://geomesa.github.io"><img align="center" width="50%" src="https://raw.githubusercontent.com/geomesa/geomesa.github.io/master/img/geomesa-2x.png"></img></a>
</p>

GeoMesa is an open source suite of tools that enables large-scale geospatial querying and analytics on distributed
computing systems. GeoMesa provides spatio-temporal indexing on top of the Accumulo, HBase, Google Bigtable and
Cassandra databases for massive storage of point, line, and polygon data. GeoMesa also provides near real time
stream processing of spatio-temporal data by layering spatial semantics on top of Apache Kafka. Through GeoServer,
GeoMesa facilitates integration with a wide range of existing mapping clients over standard OGC (Open Geospatial
Consortium) APIs and protocols such as WFS and WMS. GeoMesa supports Apache Spark for custom distributed
geospatial analytics.

<p align="center">
  <img align="center" height="150px" src="http://www.geomesa.org/img/geomesa-overview-848x250.png"></img>
</p>

#### ![LocationTech](https://pbs.twimg.com/profile_images/2552421256/hv2oas84tv7n3maianiq_normal.png) GeoMesa is a member of the [LocationTech](http://www.locationtech.org) working group of the Eclipse Foundation.

## Join the Community

* <a href="https://gitter.im/locationtech/geomesa?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge" target="_blank"><img src="https://badges.gitter.im/Join%20Chat.svg" alt="Join the chat at https://gitter.im/locationtech/geomesa"></img></a>
* GeoMesa [Users](https://locationtech.org/mhonarc/lists/geomesa-users/) and [Dev](https://locationtech.org/mhonarc/lists/geomesa-dev/) mailing lists
* GeoMesa [JIRA](https://geomesa.atlassian.net/issues/?jql=order+by+created+DESC) for issue tracking

## Documentation

* [Main documentation](http://www.geomesa.org/documentation/)
* Quick Starts:
  [HBase](http://www.geomesa.org/documentation/tutorials/geomesa-quickstart-hbase.html) |
  [Accumulo](http://www.geomesa.org/documentation/tutorials/geomesa-quickstart-accumulo.html) |
  [Cassandra](http://www.geomesa.org/documentation/tutorials/geomesa-quickstart-cassandra.html) |
  [Kafka](http://www.geomesa.org/documentation/tutorials/geomesa-quickstart-kafka.html) |
  [Redis](http://www.geomesa.org/documentation/tutorials/geomesa-quickstart-redis.html) |
  [FileSystem](http://www.geomesa.org/documentation/current/tutorials/geomesa-quickstart-fsds.html)
 
* [Tutorials](http://www.geomesa.org/tutorials/)

## Downloads

**Current release: 2.3.0**

  &nbsp;&nbsp;&nbsp;&nbsp;
  [**HBase**](https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-2.3.0/geomesa-hbase_2.11-2.3.0-bin.tar.gz) |
  [**Accumulo**](https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-2.3.0/geomesa-accumulo_2.11-2.3.0-bin.tar.gz) |
  [**Cassandra**](https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-2.3.0/geomesa-cassandra_2.11-2.3.0-bin.tar.gz) |
  [**Kafka**](https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-2.3.0/geomesa-kafka_2.11-2.3.0-bin.tar.gz) |
  [**Redis**](https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-2.3.0/geomesa-redis_2.11-2.3.0-bin.tar.gz) |
  [**FileSystem**](https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-2.3.0/geomesa-fs_2.11-2.3.0-bin.tar.gz) |
  [**Bigtable**](https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-2.3.0/geomesa-bigtable_2.11-2.3.0-bin.tar.gz) |
  [**Source**](https://github.com/locationtech/geomesa/archive/geomesa_2.11-2.3.0.tar.gz) |
  [**CheckSums**](https://github.com/locationtech/geomesa/releases/geomesa_2.11-2.3.0)

**Development version: 2.4.0-SNAPSHOT** &nbsp;
  [![Build Status](https://api.travis-ci.org/locationtech/geomesa.svg?branch=master)](https://travis-ci.org/locationtech/geomesa)

### Verifying Downloads

Downloads hosted on GitHub include SHA-256 hashes and gpg signatures (.asc files). To verify a download using gpg,
import the appropriate key:

```bash
$ gpg2 --keyserver hkp://pool.sks-keyservers.net --recv-keys CD24F317
```

Then verify the file:

```bash
$ gpg2 --verify geomesa-accumulo_2.11-2.3.0-bin.tar.gz.asc geomesa-accumulo_2.11-2.3.0-bin.tar.gz
```

The keys currently used for signing are:

| Key ID | Name |
| ------ | ---- |
| `CD24F317` | Emilio Lahr-Vivaz &lt;elahrvivaz(-at-)ccri.com&gt; |
| `1E679A56` | James Hughes &lt;jnh5y(-at-)ccri.com&gt; |

### Upgrading

To upgrade between minor releases of GeoMesa, the versions of all GeoMesa components **must** match. 

This means that the version of the `geomesa-accumulo-distributed-runtime` JAR installed on Accumulo
tablet servers **must** match the version of the `geomesa-accumulo-gs-plugin` JAR installed in the `WEB-INF/lib`
directory of GeoServer.

See the [Upgrade Guide](http://www.geomesa.org/documentation/user/upgrade.html) for information on specific version updates.

## Maven Integration

GeoMesa is now hosted on Maven Central. However, it still depends on several third-party libraries only available
in other repositories. To include GeoMesa in your project, add the following repositories to your pom:

```xml
<repositories>
  <repository>
    <id>boundlessgeo</id>
    <url>https://repo.boundlessgeo.com/main</url>
  </repository>
  <repository>
    <id>osgeo</id>
    <url>http://download.osgeo.org/webdav/geotools</url>
  </repository>
  <repository>
    <id>conjars.org</id>
    <url>http://conjars.org/repo</url>
  </repository>
</repositories>
```

and then include the desired `geomesa-*` dependencies:

```xml
<dependency>
  <groupId>org.locationtech.geomesa</groupId>
  <artifactId>geomesa-utils_2.11</artifactId>
  <version>2.3.0</version>
</dependency>
  ...
```

To download from the LocationTech Maven repository (required for older versions), add:

```xml
<repository>
  <id>locationtech-releases</id>
  <url>https://repo.locationtech.org/content/groups/releases</url>
  <snapshots>
    <enabled>false</enabled>
  </snapshots>
</repository>
```

For snapshot integration, add:

```xml
<repository>
  <id>geomesa-snapshots</id>
  <url>https://repo.locationtech.org/content/repositories/geomesa-snapshots</url>
  <releases>
    <enabled>false</enabled>
  </releases>
  <snapshots>
    <enabled>true</enabled>
  </snapshots>
</repository>
```

## `sbt` Integration

Similarly, integration with `sbt` is straightforward:

```scala
// Add necessary resolvers
resolvers ++= Seq(
  "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
  "boundlessgeo" at "https://repo.boundlessgeo.com/main",
  "osgeo" at "http://download.osgeo.org/webdav/geotools",
  "conjars.org" at "http://conjars.org/repo"
)

// Select desired modules
libraryDependencies ++= Seq(
  "org.locationtech.geomesa" %% "geomesa-utils" % "2.3.0",
  ...
)
```

## Building from Source

Requirements:

* [Git](http://git-scm.com/)
* [Java JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Apache Maven](http://maven.apache.org/) 3.3.9 or later

Use Git to download the source code. Navigate to the destination directory, then run:

    git clone git@github.com:locationtech/geomesa.git
    cd geomesa

The project is managed by Maven. To build, run:

    mvn clean install

The full build takes quite a while. To speed it up, you may skip tests and use multiple threads. GeoMesa also
provides the script `build/mvn`, which is a wrapper around Maven that downloads and runs
[Zinc](https://github.com/typesafehub/zinc), a fast incremental compiler:

    build/mvn clean install -T8 -DskipTests

If the Zinc build fails with an error finding "javac", try setting the JAVA_HOME
environment variable to point to the root of your JDK.  Example from a Mac:

    JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_51.jdk/Contents/Home" build/mvn clean install
