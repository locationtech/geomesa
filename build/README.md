<p align="center">
  <a href="https://geomesa.github.io"><img align="center" width="50%" src="https://raw.githubusercontent.com/geomesa/geomesa.github.io/main/img/geomesa-2x.png"></img></a>
</p>

GeoMesa is an open source suite of tools that enables large-scale geospatial querying and analytics on distributed
computing systems. GeoMesa provides spatio-temporal indexing on top of the Accumulo, HBase and
Cassandra databases for massive storage of point, line, and polygon data. GeoMesa also provides near real time
stream processing of spatio-temporal data by layering spatial semantics on top of Apache Kafka. Through GeoServer,
GeoMesa facilitates integration with a wide range of existing mapping clients over standard OGC (Open Geospatial
Consortium) APIs and protocols such as WFS and WMS. GeoMesa supports Apache Spark for custom distributed
geospatial analytics.

<p align="center">
  <img align="center" height="150px" src="https://www.geomesa.org/img/geomesa-overview-848x250.png"></img>
</p>

#### ![LocationTech](https://pbs.twimg.com/profile_images/2552421256/hv2oas84tv7n3maianiq_normal.png) GeoMesa is a member of the [LocationTech](https://projects.eclipse.org/projects/locationtech.geomesa) working group of the Eclipse Foundation.

## Join the Community

* <a href="https://gitter.im/locationtech/geomesa?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge" target="_blank"><img src="https://badges.gitter.im/Join%20Chat.svg" alt="Join the chat at https://gitter.im/locationtech/geomesa"></img></a>
* GeoMesa [Users](https://accounts.eclipse.org/mailing-list/geomesa-users) and [Dev](https://accounts.eclipse.org/mailing-list/geomesa-dev) mailing lists
* GeoMesa [JIRA](https://geomesa.atlassian.net/issues/?jql=order+by+created+DESC) for issue tracking

## Documentation

* [Main documentation](https://www.geomesa.org/documentation/)
* [Upgrade Guide](https://www.geomesa.org/documentation/user/upgrade.html)
* Quick Starts:
  [HBase](https://www.geomesa.org/documentation/tutorials/geomesa-quickstart-hbase.html) |
  [Accumulo](https://www.geomesa.org/documentation/tutorials/geomesa-quickstart-accumulo.html) |
  [Cassandra](https://www.geomesa.org/documentation/tutorials/geomesa-quickstart-cassandra.html) |
  [Kafka](https://www.geomesa.org/documentation/tutorials/geomesa-quickstart-kafka.html) |
  [Redis](https://www.geomesa.org/documentation/tutorials/geomesa-quickstart-redis.html) |
  [FileSystem](https://www.geomesa.org/documentation/current/tutorials/geomesa-quickstart-fsds.html)
* [Tutorials](https://www.geomesa.org/tutorials/)

## Downloads

**Current release: [${geomesa.release.version}](https://github.com/locationtech/geomesa/releases/tag/geomesa-${geomesa.release.version})**

  &nbsp;&nbsp;&nbsp;&nbsp;
  [**HBase**](https://github.com/locationtech/geomesa/releases/download/geomesa-${geomesa.release.version}/geomesa-hbase_${scala.binary.version}-${geomesa.release.version}-bin.tar.gz) |
  [**Accumulo**](https://github.com/locationtech/geomesa/releases/download/geomesa-${geomesa.release.version}/geomesa-accumulo_${scala.binary.version}-${geomesa.release.version}-bin.tar.gz) |
  [**Cassandra**](https://github.com/locationtech/geomesa/releases/download/geomesa-${geomesa.release.version}/geomesa-cassandra_${scala.binary.version}-${geomesa.release.version}-bin.tar.gz) |
  [**Kafka**](https://github.com/locationtech/geomesa/releases/download/geomesa-${geomesa.release.version}/geomesa-kafka_${scala.binary.version}-${geomesa.release.version}-bin.tar.gz) |
  [**Redis**](https://github.com/locationtech/geomesa/releases/download/geomesa-${geomesa.release.version}/geomesa-redis_${scala.binary.version}-${geomesa.release.version}-bin.tar.gz) |
  [**FileSystem**](https://github.com/locationtech/geomesa/releases/download/geomesa-${geomesa.release.version}/geomesa-fs_${scala.binary.version}-${geomesa.release.version}-bin.tar.gz) |
  [**PostGIS**](https://github.com/locationtech/geomesa/releases/download/geomesa-${geomesa.release.version}/geomesa-gt_${scala.binary.version}-${geomesa.release.version}-bin.tar.gz)

### Verifying Downloads

Downloads hosted on GitHub include SHA-256 hashes and gpg signatures (.asc files). To verify a download using gpg,
import the appropriate key:

```bash
$ gpg2 --keyserver hkp://pool.sks-keyservers.net --recv-keys CD24F317
```

Then verify the file:

```bash
$ gpg2 --verify geomesa-accumulo_${scala.binary.version}-${geomesa.release.version}-bin.tar.gz.asc geomesa-accumulo_${scala.binary.version}-${geomesa.release.version}-bin.tar.gz
```

The keys currently used for signing are:

| Key ID | Name |
| ------ | ---- |
| `CD24F317` | Emilio Lahr-Vivaz &lt;elahrvivaz(-at-)ccri.com&gt; |
| `1E679A56` | James Hughes &lt;jnh5y(-at-)ccri.com&gt; |

## Maven Integration

[![Maven](.github/maven-badge.svg)](https://search.maven.org/search?q=g:org.locationtech.geomesa)

GeoMesa is hosted on Maven Central. To include it as a dependency, add the desired modules, for example:

```xml
<dependency>
  <groupId>org.locationtech.geomesa</groupId>
  <artifactId>geomesa-hbase-datastore_${scala.binary.version}</artifactId>
  <version>${geomesa.release.version}</version>
</dependency>
```

GeoMesa depends on several third-party libraries that are only available in separate repositories. To include
GeoMesa in your project, add the following repositories to your pom:

```xml
<repositories>
  <!-- geotools -->
  <repository>
    <id>osgeo</id>
    <url>https://repo.osgeo.org/repository/release</url>
  </repository>
  <!-- confluent -->
  <repository>
    <id>confluent</id>
    <url>https://packages.confluent.io/maven/</url>
  </repository>
</repositories>
```

### Nightly Snapshots

Snapshot versions are published nightly to the Eclipse repository:

```xml
<repository>
  <id>geomesa-snapshots</id>
  <url>https://repo.eclipse.org/content/repositories/geomesa-snapshots</url>
  <releases>
    <enabled>false</enabled>
  </releases>
  <snapshots>
    <enabled>true</enabled>
  </snapshots>
</repository>
```

### Spark Runtimes

GeoMesa publishes `spark-runtime` JARs for integration with Spark environments like Databricks. These
shaded JARs include all the required dependencies in a single artifact. When importing through Maven, all
transitive dependencies can be excluded. There are Spark runtime JARs available for most of the different
DataStore implementations:

```xml
<dependency>
  <groupId>org.locationtech.geomesa</groupId>
  <artifactId>geomesa-gt-spark-runtime_${scala.binary.version}</artifactId>
  <version>${geomesa.release.version}</version>
  <exclusions>
    <exclusion>
      <!-- if groupId wildcards are not supported, the two main ones are jline:* and org.geotools:* -->
      <groupId>*</groupId>
      <artifactId>*</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

These JARs are also included in the [Downloads](#downloads) bundles, above.

## `sbt` Integration

Similarly, integration with `sbt` is straightforward:

```scala
// Add necessary resolvers
resolvers ++= Seq(
  "osgeo" at "https://repo.osgeo.org/repository/release",
  "confluent" at "https://packages.confluent.io/maven"
)

// Select desired modules
libraryDependencies ++= Seq(
  "org.locationtech.geomesa" %% "geomesa-utils" % "${geomesa.release.version}"
)
```

## Building from Source

**Development version: ${geomesa.devel.version}**

  &nbsp;&nbsp;&nbsp;&nbsp;
  [![Build Status](https://github.com/locationtech/geomesa/actions/workflows/build-and-test-2.12.yml/badge.svg?branch=main)](https://github.com/locationtech/geomesa/actions/workflows/build-and-test-2.12.yml?query=branch%3Amain)

  &nbsp;&nbsp;&nbsp;&nbsp;
  [![Build Status](https://github.com/locationtech/geomesa/actions/workflows/build-and-test-2.13.yml/badge.svg?branch=main)](https://github.com/locationtech/geomesa/actions/workflows/build-and-test-2.13.yml?query=branch%3Amain)

  &nbsp;&nbsp;&nbsp;&nbsp;
  [![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/locationtech/geomesa/badge)](https://api.securityscorecards.dev/projects/github.com/locationtech/geomesa)

Requirements:

* [Git](https://git-scm.com/)
* [Java JDK 8](https://adoptium.net/temurin/releases/)
* [Apache Maven](https://maven.apache.org/) 3.6 or later
* [Docker](https://docs.docker.com/get-docker/) (only required for running unit tests)

Use Git to download the source code. Navigate to the destination directory, then run:

    git clone git@github.com:locationtech/geomesa.git
    cd geomesa

The project is built using Maven. To build, run:

    mvn clean install -DskipTests

The full build takes quite a while. To speed it up, you may use multiple threads (`-T 1.5C`).

To run unit tests, omit the `-DskipTests` (note: requires `docker` to be available).

### Build with Bloop Compile Server

GeoMesa also provides experimental support for the [Bloop](https://scalacenter.github.io/bloop/) compile server,
which provides fast incremental compilation. To export the GeoMesa build to Bloop, run:

    ./build/bloop-export.sh

For more information on using Bloop, refer to the
[Bloop documentation](https://scalacenter.github.io/bloop/docs/build-tools/maven).

### Build with Zinc Compile Server

GeoMesa also provides experimental support for the [Zinc](https://github.com/typesafehub/zinc) compile server,
which provides fast incremental compilation. However, please note that Zinc is no longer actively maintained.
To use an existing Zinc server, run maven with `-Pzinc`. GeoMesa provides a helper script at `build/mvn`, which
is a wrapper around Maven that downloads and runs Zinc automatically:

    build/mvn clean install -T8 -DskipTests

If the Zinc build fails with an error finding "javac", try setting the JAVA_HOME
environment variable to point to the root of your JDK. Example from a Mac:

    JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_51.jdk/Contents/Home" build/mvn clean install

### Scala Cross Build

To build for a different Scala version (e.g. 2.13), run the following script, then build as normal:

    ./build/change-scala-version.sh 2.13

### Building on OS X

When building on OS X and using Docker Desktop in a non-default configuration, you may need to edit `~/.testcontainers.properties` to contain the following:

```
docker.client.strategy=org.testcontainers.dockerclient.UnixSocketClientProviderStrategy
```
