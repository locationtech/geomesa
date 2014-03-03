[![Build Status](https://travis-ci.org/locationtech/geomesa.png?branch=master)](https://travis-ci.org/locationtech/geomesa)

# geomesa Project Structure

There are a few sub-projects which correspond to the main aims of this project.

### geomesa-core

This sub-project contains the code which runs remotely on the "cloud", in this
case the Accumulo key-value store.

Changes here require that you upload the geomesa-distributed-runtime to the cloud
(specifically, to the tablet servers' lib/ext directory).
The other two projects also depend on this one, so depending on your change you will need to update them as well.

#### Scala console via scala-maven-plugin

Since it is reasonable to expect that a developer may wish to use/test core functions directly, the Scala console is
spun into this project's pom.

You can invoke it one of two ways

From the root directory by specifying geomesa-core
```geomesa> mvn -pl geomesa-core scala:console```

Or from the sub-project's directory
```geomesa-core> mvn scala:console```

By default, all of the project packages in ```core``` are loaded along with JavaConversions, JavaConverters.

### geomesa-distributed-runtime

This sub-project assembled a jar with dependencies that must be distributed to Accumulo tablet servers lib/ext
directory.

### geomesa-plugin

This sub-project creates a plugin which provides WFS and WMS support.

The JAR named geomesa-plugin-<Version>-geoserver-plugin.jar is ready to be deployed in GeoServer by
copying it into geoserver/WEB-INF/lib/

### geomesa-utils

This sub-project stores our GeoHash implementation and other general library functions unrelated to
Accumulo.

This sub-project contains any helper tools for geomesa.  Some of these tools such as
the GeneralShapefileIngest have Map/Reduce components, so the geomesa-utils JAR lives on HDFS.

### geomesa-dist

This sub-project contains the distribution-ready TAR-ball as well as the
documentation (in DocBook form, rendered to PDF).

##  Checkout out and build GeoMesa

* Navigate to where you would like to download this project.
* git clone git@github.com:geomesa/geomesa.git

## Building Instructions

This project is managed by Maven, and builds with the command

```geomesa> mvn clean install```

From the root directory, this builds each sub-project with its additional dependencies-included JAR.

