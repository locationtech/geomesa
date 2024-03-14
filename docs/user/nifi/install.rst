.. _nifi_install:

Installation
------------

.. note::

    The examples below expect a version to be set in the environment:

    .. parsed-literal::

        $ export TAG="|release_version|"
        $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version

Get the Processors
~~~~~~~~~~~~~~~~~~

.. note::

    There are GeoMesa NARs available for both Scala 2.12 and 2.13. Generally, this is only relevant if using
    custom GeoMesa converter modules written in Scala. Otherwise, either version can be used as long as all
    GeoMesa NARs in a given NiFi use the same Scala version.

The GeoMesa NiFi processors are available for download from `GitHub <https://github.com/geomesa/geomesa-nifi/releases>`__.

Alternatively, you may build the processors from source. First, clone the project from GitHub. Pick a reasonable
directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-nifi.git
    $ cd geomesa-nifi

To build the project, run:

.. code-block:: bash

    $ mvn clean install

The nar contains bundled dependencies. To change the dependency versions, modify the version properties
(``<hbase.version>``, etc) in the ``pom.xml`` before building.

Install the Processors
~~~~~~~~~~~~~~~~~~~~~~

To install the GeoMesa processors you will need to copy the NAR files into the ``lib`` directory of your
NiFi installation. There two common NAR files, and seven datastore-specific NAR files.

Common NAR files:

* ``geomesa-datastore-services-api-nar_$VERSION.nar``
* ``geomesa-datastore-services-nar_$VERSION_.nar``

Datastore NAR files:

* ``geomesa-hbase2-nar_$VERSION.nar`` - HBase 2.5
* ``geomesa-accumulo20-nar_$VERSION.nar`` - Accumulo 2.0
* ``geomesa-accumulo21-nar_$VERSION.nar`` - Accumulo 2.1
* ``geomesa-gt-nar_$VERSION.nar`` - PostGIS
* ``geomesa-kafka-nar_$VERSION.nar`` Kafka
* ``geomesa-redis-nar_$VERSION.nar`` Redis
* ``geomesa-fs-nar_$VERSION.nar`` Hadoop
* ``geomesa-lambda-nar_$VERSION.nar`` Kafka (lambda architecture)

The common NAR files are required for all datastores. The datastore-specific NARs can be installed as needed.

.. note::

  There are two HBase and Accumulo NARs that correspond to different versions.
  Be sure to choose the appropriate NAR for your database.

If you downloaded the NARs from GitHub:

.. code-block:: bash

    $ export NARS="geomesa-hbase2-nar geomesa-datastore-services-api-nar geomesa-datastore-services-nar"
    $ for nar in $NARS; do wget "https://github.com/geomesa/geomesa-nifi/releases/download/geomesa-nifi-$TAG/$nar_$VERSION.nar"; done
    $ mv *.nar $NIFI_HOME/extensions/

Or, to install the NARs after building from source:

.. code-block:: bash

    $ export NARS="geomesa-hbase2-nar geomesa-datastore-services-api-nar geomesa-datastore-services-nar"
    $ for nar in $NARS; do find . -name $nar_$VERSION.nar -exec cp {} $NIFI_HOME/extensions/ \;; done
