GeoMesa ScyllaDB Quick Start
=============================

This tutorial is the fastest and easiest way to get started with GeoMesa using ScyllaDB.

.. note::

    ScyllaDB is a drop-in replacement for Apache Cassandra that is fully compatible with
    the Cassandra Query Language (CQL) and wire protocol. Because of this wire-level
    compatibility, **GeoMesa's Cassandra DataStore, tools, and tutorials work identically
    with ScyllaDB** - you simply point the connection parameters to your ScyllaDB cluster
    instead of a Cassandra cluster.

About ScyllaDB Support
----------------------

Since ScyllaDB implements the same CQL wire protocol as Cassandra, the ScyllaDB quick start
uses the **same code, JAR files, and command-line tools** as the Cassandra quick start. The only
difference is that you configure the connection to point to a ScyllaDB instance instead of Cassandra.

This means:

- The tutorial code is in the Cassandra quickstart module
- The JAR file is named ``geomesa-tutorials-cassandra-quickstart-{{release}}.jar``
- The main class is ``org.geomesa.example.cassandra.CassandraQuickStart``
- The GeoMesa DataStore uses Cassandra client libraries (which work with ScyllaDB)

Prerequisites
-------------

Before you begin, you must have the following installed and configured:

- `Java <https://adoptium.net/temurin/releases/>`__ JDK {{java_required_version}}
- Apache `Maven <https://maven.apache.org/>`__ {{maven_required_version}}
- a GitHub client
- a ScyllaDB {{scylladb_supported_versions}} instance, either standalone or cluster

Setting Up ScyllaDB
-------------------

Quick Start with Docker
~~~~~~~~~~~~~~~~~~~~~~~

The easiest way to get started with ScyllaDB is using Docker:

.. code-block:: bash

    $ docker run --name scylla -d -p 9042:9042 scylladb/scylla:latest

This starts a ScyllaDB container listening on the default CQL port (9042).

Create a ScyllaDB Keyspace
~~~~~~~~~~~~~~~~~~~~~~~~~~

You will need a keyspace in ScyllaDB to contain the tutorial tables. Connect using ``cqlsh``:

.. code-block:: bash

    $ docker exec -it scylla cqlsh

Then create a keyspace:

.. code-block:: cql

    CREATE KEYSPACE geomesa WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 1};

This creates a keyspace called "geomesa" that will contain all GeoMesa data, including
spatial features and associated metadata.

.. note::

    For production deployments, use a higher replication factor and consider
    using ``NetworkTopologyStrategy`` instead of ``SimpleStrategy``.

Running the Tutorial
--------------------

The ScyllaDB quick start follows the **exact same steps** as the Cassandra quick start, with one key
difference: you point the connection parameters to your ScyllaDB cluster instead of Cassandra.

Follow the complete tutorial at :doc:`geomesa-quickstart-cassandra`, using these connection parameters
for ScyllaDB:

- ``--cassandra.contact.point`` - your ScyllaDB host and port (e.g., ``localhost:9042`` or ``127.0.0.1:9042`` for Docker)
- ``--cassandra.keyspace`` - your ScyllaDB keyspace (e.g., ``geomesa`` if you followed the setup above)
- ``--cassandra.catalog`` - the table name for storing features

Example command:

.. code-block:: bash

    $ java -cp geomesa-tutorials-cassandra/geomesa-tutorials-cassandra-quickstart/target/geomesa-tutorials-cassandra-quickstart-{{release}}.jar \
        org.geomesa.example.cassandra.CassandraQuickStart \
        --cassandra.contact.point localhost:9042 \
        --cassandra.keyspace geomesa            \
        --cassandra.catalog quickstart

The tutorial will:

1. Create a schema in ScyllaDB for spatial data
2. Write sample GDELT features to ScyllaDB
3. Run spatial and temporal queries
4. Display the results

Visualization
-------------

All visualization options from the Cassandra quick start work identically with ScyllaDB:

- **Leaflet Export**: Use ``bin/geomesa-cassandra export`` with your ScyllaDB connection parameters
- **GeoServer**: Use the ``Cassandra (GeoMesa)`` data source in GeoServer, pointing to your ScyllaDB cluster

See the :doc:`geomesa-quickstart-cassandra` for detailed visualization instructions.

Next Steps
----------

For more information on using GeoMesa with ScyllaDB, see:

- :doc:`../user/scylladb/index` - ScyllaDB overview and features
- :doc:`../user/scylladb/install` - Installation and configuration details
- :doc:`../user/scylladb/usage` - Usage examples and migration from Cassandra
- :doc:`../user/cassandra/index` - Complete Cassandra DataStore documentation (applies to ScyllaDB)
