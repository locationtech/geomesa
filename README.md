<p align="center"><a href="http://geomesa.github.io"><img src="https://raw.githubusercontent.com/geomesa/geomesa.github.io/master/img/geomesa-2x.png"></img></a></p>

### Build Status

Version | Status
------ | -----
| 1.x for Accumulo 1.4.x | [![Build Status](https://travis-ci.org/locationtech/geomesa.svg?branch=accumulo1.4%2F1.x)](https://travis-ci.org/locationtech/geomesa)  	| 
| 1.x for Accumulo 1.5.x | [![Build Status](https://travis-ci.org/locationtech/geomesa.svg?branch=accumulo1.5.x%2F1.x)](https://travis-ci.org/locationtech/geomesa)  	| 

## GeoMesa

![Splash](http://geomesa.github.io/img/geomesa-overview-848x250.png)

GeoMesa is an open-source, distributed, spatio-temporal database built on top of the Apache Accumulo column family store. GeoMesa implements standard Geotools interfaces to provide geospatial functionality over very large data sets to application developers.  GeoMesa provides plugins for exposing geospatial data stored in Accumulo via standards-based OGC HTTP services and cluster monitoring and management tools within the GeoServer administrative interface.  

#### ![LocationTech](https://pbs.twimg.com/profile_images/2552421256/hv2oas84tv7n3maianiq_normal.png) GeoMesa is a member of the [LocationTech](http://www.locationtech.org) working group of the Eclipse Foundation.


## Building Instructions

* Navigate to where you would like to download this project.
* git clone git@github.com:geomesa/geomesa.git

This project is managed by Maven, and builds with the command

```geomesa> mvn clean install```

From the root directory, this builds each sub-project with its additional dependencies-included JAR.

## Documentation

* [Quick Start](http://geomesa.github.io/#quick-start) on the main [documentation](http://geomesa.github.io) site.
* [FAQ](http://geomesa.github.io/faq/)
* [Tutorials](http://geomesa.github.io)
* GeoMesa [Users](https://locationtech.org/mhonarc/lists/geomesa-users/) and [Dev](https://locationtech.org/mhonarc/lists/geomesa-dev/) mailing list archives

## GeoMesa Project Structure

#### geomesa-core

This project contains the implementations of the core indexing structures, Accumulo iterators, and the GeoTools interfaces for exposing the functionalit yas a ```DataStore``` to both application developers and GeoServer.

##### Scala console via scala-maven-plugin

To test and interact with core functionality, the Scala console can be invoked in a couple of ways.  From the root directory by specifying geomesa-core ```geomesa> mvn -pl geomesa-core scala:console```.  Or from the sub-project's directory ```geomesa-core> mvn scala:console```.  By default, all of the project packages in ```core``` are loaded along with JavaConversions, JavaConverters.

#### geomesa-distributed-runtime

This sub-project assembles a jar with dependencies that must be distributed to Accumulo tablet servers lib/ext
directory or to an HDFS directory where Accumulo's VFSClassLoader can pick it up.

#### geomesa-plugin

This sub-project creates a plugin which provides WFS and WMS support.  The JAR named geomesa-plugin-<Version>-geoserver-plugin.jar is ready to be deployed in GeoServer by copying it into geoserver/WEB-INF/lib/

#### geomesa-utils

This sub-project stores our GeoHash implementation and other general library functions unrelated to Accumulo. This sub-project contains any helper tools for geomesa.  Some of these tools such as the GeneralShapefileIngest have Map/Reduce components, so the geomesa-utils JAR lives on HDFS.

#### geomesa-dist

This sub-project contains the distribution-ready TAR-ball as well as the documentation (in DocBook form, rendered to PDF).

#### geomesa-compute

This sub-project contains utilities for working with distributed computing environments.  Currently, there are methods for instantiating an Apache Spark Resilient Distributed Dataset from a CQL query against data stored in GeoMesa.  Eventually, this project will contain bindings for traditional map-reduce processing, Scalding, and other environments.

