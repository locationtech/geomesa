GeoMesa Development
===================

This chapter describes how to build GeoMesa from source and provides an
overview of the process of developing GeoMesa.

Using Maven
-----------

The GeoMesa project uses `Apache Maven <https://maven.apache.org/>`__ as a build tool. The Maven project's `Maven in 5 Minutes <https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html>`__ provides a quick introduction to getting started with its `mvn` executable.

.. _building_from_source:

Building from Source
--------------------

These development tools are required:

* `Java JDK 8 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__,
* `Apache Maven <http://maven.apache.org/>`__ |maven_version|, and
* `Git <https://git-scm.com/>`__.

The GeoMesa source distribution may be cloned from GitHub:

.. code-block:: bash

    $ git clone https://github.com/locationtech/geomesa.git
    $ cd geomesa

This downloads the latest development version. To check out the code for the latest stable release
(``$VERSION`` = |release|):

.. code-block:: bash

    $ git checkout tags/geomesa-$VERSION -b geomesa-$VERSION 

Building and dependency management for GeoMesa is handled by Maven (http://maven.apache.org/). 
The Maven ``pom.xml`` file in the root directory of the source distribution contains an explicit
list of dependent libraries that will be bundled together for each module of the program.

.. note::

    The only reason dependent libraries are bundled into the final JAR is to make it easier 
    to deploy files rather than setting the classpath. If you would rather not bundle these 
    dependencies, mark them as "provided" in the POM, and update your classpath as appropriate.

The versions of Accumulo supported in GeoMesa 1.3.x are Accumulo |accumulo_version|:

.. code-block:: bash

    $ mvn clean install

The `skipTests` property may be used to speed compilation. Set it to ``true``
to omit the test phase of the build process:

.. code-block:: bash

    $ mvn clean install -DskipTests=true

To compile for Bigtable, use the ``bigtable`` profile.

.. code-block:: bash

    $ mvn clean install -Pbigtable

The ``build/mvn`` script is a wrapper around Maven that builds the project using the Zinc
(https://github.com/typesafehub/zinc) incremental compiler:

.. code-block:: bash

    $ build/mvn clean install

Scala
-----

For the most part, GeoMesa is written in `Scala <http://www.scala-lang.org/>`__,
and is compiled with Scala 2.11.7.

Using the Scala Console
^^^^^^^^^^^^^^^^^^^^^^^

To test and interact with core functionality, the Scala console can be invoked in a couple of ways. For example, by
running this command in the root source directory:

.. code-block:: bash

    $ cd geomesa-accumulo
    $ mvn -pl geomesa-accumulo-datastore scala:console

The Scala console will start, and all of the project packages in ``geomesa-accumulo-datastore`` will be loaded along
with ``JavaConversions`` and ``JavaConverters``.

Scala 2.10 Support
^^^^^^^^^^^^^^^^^^

GeoMesa uses Scala 2.11 by default. To build for Scala 2.10, run:

.. code-block:: bash

    $ build/change-scala-version.sh 2.10

This will update the project poms to publish artifacts with a `_2.10` suffix. Then build normally using Maven.

GeoMesa Project Structure
-------------------------

* **geomesa-accumulo**: the implementations of the core Accumulo indexing structures, Accumulo iterators, and the GeoTools interfaces for exposing the functionality as a ``DataStore`` to both application developers and GeoServer. Assembles a jar with dependencies that must be distributed to Accumulo tablet servers lib/ext directory or to an HDFS directory where Accumulo's VFSClassLoader can pick it up.
* **geomesa-accumulo-compute**: utilities for working with distributed computing environments. Currently, there are methods for instantiating an Apache Spark Resilient Distributed Dataset from a CQL query against data stored in GeoMesa. Eventually, this project will contain bindings for traditional map-reduce processing and other environments.
* **geomesa-accumulo-gs-plugin**: provides WFS and WMS support for GeoServer. Install by extracting into geoserver/WEB-INF/lib/.
* **geomesa-accumulo-jobs**: map/reduce jobs for maintaining GeoMesa.
* **geomesa-accumulo-raster**: adds support for ingesting and working with geospatially-referenced raster data in GeoMesa.
* **geomesa-blobstore**: an Accumulo-based store  designed to store and retrieve files which have spatio-temporal data associated with them.
* **geomesa-cassandra**: an implementation of GeoMesa on Apache Cassandra.
* **geomesa-cassandra-gs-plugin**: provides WFS and WMS support for GeoServer. Install by extracting into geoserver/WEB-INF/lib/.
* **geomesa-convert**: a configurable and extensible library for converting data into SimpleFeatures.
* **geomesa-features**: includes code for serializing SimpleFeatures and custom SimpleFeature implementations designed for GeoMesa.
* **geomesa-filter**: a library for manipulating and working with GeoTools Filters.
* **geomesa-hbase**: an implementation of GeoMesa on HBase and Google Cloud Bigtable.
* **geomesa-hbase-gs-plugin**: provides WFS and WMS support for GeoServer. Install by extracting into geoserver/WEB-INF/lib/.
* **geomesa-index-api**: common structure and methods for indexing and querying simple features.
* **geomesa-kafka**: an implementation of GeoMesa in Kafka for maintaining near-real-time caches of streaming data.
* **geomesa-kafka-gs-plugin**: provides WFS and WMS support for GeoServer. Install by extracting into geoserver/WEB-INF/lib/.
* **geomesa-logger**: logging facade for scala version compatibility.
* **geomesa-metrics**: extensions and configuration for dropwizard metrics integration.
* **geomesa-native-api**: a non-GeoTools-based API for persisting and querying data in GeoMesa Accumulo.
* **geomesa-process**: analytic processes optimized on GeoMesa data stores.
* **geomesa-security**: adds support for managing security and authorization levels for data stored in GeoMesa.
* **geomesa-stream**: a GeoMesa library that provides tools to process streams of `SimpleFeatures`.
* **geomesa-tools**: a set of command line tools for managing features, ingesting and exporting data, configuring tables, and explaining queries in GeoMesa.
* **geomesa-utils**: stores our GeoHash implementation and other general library functions unrelated to Accumulo. This sub-project contains any helper tools for geomesa. Some of these tools such as the GeneralShapefileIngest have Map/Reduce components, so the geomesa-utils JAR lives on HDFS.
* **geomesa-web**: web services for accessing GeoMesa.
* **geomesa-z3**: the implementation of Z3, GeoMesa's space-filling Z-order curve.
