Installing GeoMesa ScyllaDB
===========================

.. note::

    GeoMesa currently supports ScyllaDB version |scylladb_version|.

.. note::

    The examples below expect a version to be set in the environment:

    .. parsed-literal::
        $ export TAG="|release_version|"
        $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version

Connecting to ScyllaDB
-----------------------

The first step to getting started with ScyllaDB and GeoMesa is to install
ScyllaDB itself. You can find good directions for downloading and installing
ScyllaDB online. For example, see Scylla's official `getting started`_ documentation.

.. _getting started: https://opensource.docs.scylladb.com/stable/getting-started/index.html

Quick Start with Docker
~~~~~~~~~~~~~~~~~~~~~~~

The easiest way to get started with ScyllaDB is using Docker:

.. code-block:: bash

    $ docker run --name scylla -d -p 9042:9042 scylladb/scylla:latest

This starts a ScyllaDB container listening on the default CQL port (9042).

Once ScyllaDB is running, create a keyspace for GeoMesa. Connect using ``cqlsh``:

.. code-block:: bash

    $ docker exec -it scylla cqlsh

Then create a keyspace:

.. code-block:: cql

    CREATE KEYSPACE IF NOT EXISTS geomesa_scylla WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

.. note::

    For production deployments, use a higher replication factor and consider
    using ``NetworkTopologyStrategy`` instead of ``SimpleStrategy``.

Setting Up for GeoMesa
~~~~~~~~~~~~~~~~~~~~~~~

After creating the keyspace, note your ScyllaDB contact point. For a local Docker installation,
this will be ``127.0.0.1:9042``. You can verify the port in the ScyllaDB configuration.

Since ScyllaDB is wire-compatible with Cassandra, GeoMesa uses the same libraries and
configuration. Set the ``CASSANDRA_HOME`` environment variable to point to a Cassandra
installation, or let GeoMesa download the necessary JARs automatically (see below).

Installing GeoMesa Tools and Plugins
------------------------------------

ScyllaDB uses the **same GeoMesa Cassandra distribution** for all components including:

* Binary distribution and command line tools
* Building from source
* GeoServer plugins

Please refer to the :ref:`Cassandra installation documentation <cassandra_install_source>` for complete
instructions on:

* :ref:`Installing from the binary distribution <cassandra_install_source>`
* :ref:`Building from source <cassandra_install_source>`
* :ref:`Setting up command line tools <setting_up_cassandra_commandline>`
* :ref:`Installing the GeoServer plugin <install_cassandra_geoserver>`

.. note::

    All ``geomesa-cassandra`` command line tools work identically with ScyllaDB. Simply point them
    at your ScyllaDB cluster using the contact point parameter.

.. note::

    The same Cassandra client JARs work with both Cassandra and ScyllaDB due to wire protocol compatibility.
    When using ``$CASSANDRA_HOME`` or installing dependencies, use the Cassandra client libraries as documented
    in the Cassandra installation guide.