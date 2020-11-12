Installation
------------

Get the Processors
~~~~~~~~~~~~~~~~~~

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

To install the GeoMesa processors you will need to copy the nar files into the ``lib`` directory of your
NiFi installation. There currently two common nar files, and seven datastore-specific nar files.

Common nar files:

* ``geomesa-datastore-services-api-nar-$VERSION.nar``
* ``geomesa-datastore-services-nar-$VERSION.nar``

Datastore nar files:

* ``geomesa-kafka-nar-$VERSION.nar``
* ``geomesa-hbase1-nar-$VERSION.nar``
* ``geomesa-hbase2-nar-$VERSION.nar``
* ``geomesa-redis-nar-$VERSION.nar``
* ``geomesa-accumulo1-nar-$VERSION.nar``
* ``geomesa-accumulo2-nar-$VERSION.nar``
* ``geomesa-fs-nar-$VERSION.nar``

The common nar files are required for all datastores. The datastore-specific nars can be installed as needed.

.. note::

  There are two HBase and Accumulo nars that correspond to HBase/Accumulo 1.x and HBase/Accumulo 2.x, respectively.
  Be sure to choose the appropriate nar for your database version.

If you downloaded the nars from GitHub:

.. code-block:: bash

    $ export NARS="geomesa-hbase2-nar geomesa-datastore-services-api-nar geomesa-datastore-services-nar"
    $ for nar in $NARS; do wget "https://github.com/geomesa/geomesa-nifi/releases/download/geomesa-nifi-$VERSION/$nar-$VERSION.nar"; done
    $ mv *.nar $NIFI_HOME/lib/

Or, to install the nars after building from source:

.. code-block:: bash

    $ export NARS="geomesa-hbase2-nar geomesa-datastore-services-api-nar geomesa-datastore-services-nar"
    $ for nar in $NARS; do find . -name $nar-$VERSION.nar -exec cp {} $NIFI_HOME/lib/ \;; done
