GeoMesa Development
===================

This chapter describes how to build GeoMesa from source, and provides an
overview for writing your own Java or Scala software that makes use of GeoMesa.

Using Maven
-----------

The GeoMesa project uses `Apache Maven <https://maven.apache.org/>`_ as a build tool. The Maven project's `Maven in 5 Minutes <https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html>`_ provides a quick introduction to getting started with its `mvn` executable.

.. _building_from_source:

Building from Source
--------------------

These development tools are required:

* `Java JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_,
* `Apache Maven <http://maven.apache.org/>`_ 3.2.2 or better, and
* `Git <https://git-scm.com/>`_.

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

The version of Accumulo supported is controlled by the `accumulo-1.5` 
property; to target Accumulo 1.5:   

.. code-block:: bash

    $ mvn clean install -Daccumulo-1.5

If the property is omitted, support for Accumulo 1.6 is assumed:

.. code-block:: bash

    $ mvn clean install

The `skipTests` property may be used to speed compilation. Set it to ``true``
to omit the test phase of the build process:

.. code-block:: bash

    $ mvn clean install -DskipTests=true

The ``build/mvn`` script is a wrapper around Maven that builds the project using the Zinc
(https://github.com/typesafehub/zinc) incremental compiler:

.. code-block:: bash

    $ build/mvn clean install -Daccumulo-1.5  # Accumulo 1.5
    $ build/mvn clean install                 # Accumulo 1.6

Using the Scala Console
-----------------------

To test and interact with core functionality, the Scala console can be invoked in a couple of ways. For example, by
running this command in the root source directory:

.. code-block:: bash

    $ cd geomesa-accumulo
    $ mvn -pl geomesa-accumulo-datastore scala:console

The Scala console will start, and all of the project packages in ``geomesa-accumulo-datastore`` will be loaded along
with ``JavaConversions`` and ``JavaConverters``.

GeoMesa Project Structure
-------------------------

* **geomesa-accumulo/geomesa-accumulo-datastore**: the implementations of the core Accumulo indexing structures, Accumulo iterators, and the GeoTools interfaces for exposing the functionality as a ``DataStore`` to both application developers and GeoServer. Assembles a jar with dependencies that must be distributed to Accumulo tablet servers lib/ext directory or to an HDFS directory where Accumulo's VFSClassLoader can pick it up.
* **geomesa-blobstore**: an Accumulo-based store  designed to store and retrieve files which have spatio-temporal data associated with them. 
* **geomesa-compute**: utilities for working with distributed computing environments. Currently, there are methods for instantiating an Apache Spark Resilient Distributed Dataset from a CQL query against data stored in GeoMesa. Eventually, this project will contain bindings for traditional map-reduce processing, Scalding, and other environments.
* **geomesa-convert**: a configurable and extensible library for converting data into SimpleFeatures.
* **geomesa-dist**: packages the GeoMesa distributed runtime, GeoMesa GeoServer plugin, and GeoMesa Tools. You can manually assemble using the ``assemble.sh`` script contained in the module.
* **geomesa-examples**: includes Developer quickstart tutorials and examples for how to work with GeoMesa in Accumulo and Kafka.
* **geomesa-features**: includes code for serializing SimpleFeatures and custom SimpleFeature implementations designed for GeoMesa.
* **geomesa-filter**: a library for manipulating and working with GeoTools Filters.
* **geomesa-gs-plugin**: packages plugins which provide WFS and WMS support for various ``DataStore`` types including 
  Accumulo, BigTable, Kafka, and stream ``DataStore``\ s. 
* **geomesa-hbase**: an implementation of GeoMesa on HBase and Google Cloud Bigtable.
* **geomesa-jobs**: map/reduce and scalding jobs for maintaining GeoMesa.
* **geomesa-kafka/geomesa-kafka-datastore**: an implementation of GeoMesa in Kafka for maintaining near-real-time caches of streaming data.
* **geomesa-process**: analytic processes optimized on GeoMesa data stores.
* **geomesa-raster**: adds support for ingesting and working with geospatially-referenced raster data in GeoMesa.
* **geomesa-security**: adds support for managing security and authorization levels for data stored in GeoMesa. 
* **geomesa-stream**: a GeoMesa library that provides tools to process streams of `SimpleFeatures`.
* **geomesa-tools**: a set of command line tools for managing features, ingesting and exporting data, configuring tables, and explaining queries in GeoMesa.
* **geomesa-utils**: stores our GeoHash implementation and other general library functions unrelated to Accumulo. This sub-project contains any helper tools for geomesa. Some of these tools such as the GeneralShapefileIngest have Map/Reduce components, so the geomesa-utils JAR lives on HDFS.
* **geomesa-web**: web services for accessing GeoMesa.
* **geomesa-z3**: the implementation of Z3, GeoMesa's space-filling Z-order curve.

