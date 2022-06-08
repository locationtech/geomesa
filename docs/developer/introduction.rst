GeoMesa Development
===================

This chapter describes how to build GeoMesa from source and provides an
overview of the process of developing GeoMesa.

Using Maven
-----------

The GeoMesa project uses `Apache Maven <https://maven.apache.org/>`__ as a build tool. The Maven project's
`Maven in 5 Minutes <https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html>`__ provides a
quick introduction to getting started with its ``mvn`` executable.

.. _building_from_source:

Building from Source
--------------------

These development tools are required:

* `Java JDK 8`_
* `Apache Maven <http://maven.apache.org/>`__ |maven_version|
* A ``git`` `client <http://git-scm.com/>`__

The GeoMesa source distribution may be cloned from GitHub:

.. code-block:: bash

    $ git clone https://github.com/locationtech/geomesa.git
    $ cd geomesa

This downloads the latest development version. To check out the code for the latest stable release
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
(``$VERSION`` = |release_version_literal|):
=======
(``$VERSION`` = |release_version|):
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
(``$VERSION`` = |release_version_literal|):
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
(``$VERSION`` = |release_version_literal|):
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e9985047 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c291 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67b361c856 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
(``$VERSION`` = |release_version_literal|):
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 63db7d154a (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
(``$VERSION`` = |release_version_literal|):
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')

.. code-block:: bash

    $ git checkout tags/geomesa-$VERSION -b geomesa-$VERSION
=======
(``$VERSION`` = |release_version|):

.. code-block:: bash

    $ git checkout tags/geomesa-$VERSION -b geomesa-$VERSION 
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c5e1827657 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8cbe155de6 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db977 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 76618c8da3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
<<<<<<< HEAD
>>>>>>> 0fbf9e83e2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 89bdd3013e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c8a325e13c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> f9e8439b09 (GEOMESA-3176 Docs - fix download links in install instructions)
(``$VERSION`` = |release_version_literal|):
=======
(``$VERSION`` = |release_version|):
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
(``$VERSION`` = |release_version_literal|):
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
(``$VERSION`` = |release_version_literal|):
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 15e998504 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
(``$VERSION`` = |release_version_literal|):
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f55 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 63db7d154 (GEOMESA-3176 Docs - fix download links in install instructions)

.. code-block:: bash

    $ git checkout tags/geomesa-$VERSION -b geomesa-$VERSION
>>>>>>> 2aa923cc97 (GEOMESA-3176 Docs - fix download links in install instructions)

Building and dependency management for GeoMesa is handled by `Maven <http://maven.apache.org/>`__.
The Maven ``pom.xml`` file in the root directory of the source distribution contains a
list of dependent libraries that will be bundled together for each module of the program. Build using the
``mvn`` executable:

.. code-block:: bash

    $ mvn clean install

The `skipTests` property may be used to reduce build time, as it omits the test phase of the build process:

.. code-block:: bash

    $ mvn clean install -DskipTests

To compile for Google Bigtable, use the ``bigtable`` profile:

.. code-block:: bash

    $ mvn clean install -Pbigtable

The ``build/mvn`` script is a wrapper around Maven that builds the project using the
`Zinc <https://github.com/typesafehub/zinc>`__ incremental compiler:

.. code-block:: bash

    $ build/mvn clean install -DskipTests

Scala
-----

For the most part, GeoMesa is written in `Scala <http://www.scala-lang.org/>`__,
and is compiled with Scala 2.11.7.

Using the Scala Console
^^^^^^^^^^^^^^^^^^^^^^^

To test and interact with core functionality, the Scala console can be invoked in a couple of ways. For example, by
running this command in the root source directory:

.. code-block:: bash

    $ mvn scala:console -pl geomesa-accumulo/geomesa-accumulo-datastore

The Scala console will start, and all of the project packages in ``geomesa-accumulo-datastore`` will be loaded along
with ``JavaConversions`` and ``JavaConverters``.

GeoMesa Project Structure
-------------------------

* **geomesa-accumulo**: ``DataStore`` implementation for Apache Accumulo
* **geomesa-archetypes**: Template modules for Maven builds
* **geomesa-arrow**: Apache Arrow integration and ``DataStore`` implementation
* **geomesa-bigtable**: ``DataStore`` implementation for Google Bigtable
* **geomesa-cassandra**: ``DataStore`` implementation for Apache Cassandra
* **geomesa-convert**: Configurable and extensible library for converting arbitrary data into ``SimpleFeature``\ s
* **geomesa-features**: Custom implementations and serialization of ``SimpleFeature``\ s
* **geomesa-filter**: Library for manipulating and working with GeoTools ``Filter``\ s
* **geomesa-fs**: ``DataStore`` implementation for flat files
* **geomesa-geojson**: API and REST-ful web service for working directly with GeoJSON
* **geomesa-hbase**: ``DataStore`` implementation for Apache HBase
* **geomesa-index-api**: Core indexing and ``DataStore`` code
* **geomesa-jobs**: Map/reduce integration
* **geomesa-jupyter**: Jupyter notebook integration
* **geomesa-kafka**: ``DataStore`` implementation for Apache Kafka, for near-real-time streaming data
* **geomesa-lambda**: ``DataStore`` implementation that seamlessly uses Kafka for frequent updates and Accumulo for long-term persistence
* **geomesa-memory**: In-memory indexing code
* **geomesa-process**: Analytic processes optimized for GeoMesa stores
* **geomesa-security**: API for managing security and authorization levels in GeoMesa
* **geomesa-spark**: Apache Spark integration
* **geomesa-stream**: ``DataStore`` implementation that reads features from arbitrary URLs
* **geomesa-tools**: Command-line tools for ingesting, querying and managing data in GeoMesa
* **geomesa-utils**: Common utility code
* **geomesa-web**: REST-ful web services for integrating with GeoMesa
* **geomesa-z3**: Z3 space-filling-curve implementation
* **geomesa-zk-utils**: Zookeeper utility code
