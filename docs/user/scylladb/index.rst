ScyllaDB Data Store
===================

.. note::

    GeoMesa currently supports ScyllaDB {{scylladb_supported_versions}}.

The GeoMesa ScyllaDB Data Store is an implementation of the GeoTools
``DataStore`` interface that is backed by `ScyllaDB`_.
It is found in the ``geomesa-cassandra`` directory of the GeoMesa
source distribution, as ScyllaDB is a high-performance, drop-in replacement
for Apache Cassandra.

ScyllaDB offers superior performance and scalability compared to Cassandra,
while maintaining full compatibility with the Cassandra Query Language (CQL)
and wire protocol. This means GeoMesa can use ScyllaDB without any code changes,
simply by connecting to a ScyllaDB cluster instead of Cassandra.

To get started with the ScyllaDB Data Store, try the :doc:`/tutorials/geomesa-quickstart-scylladb`.

.. _ScyllaDB: https://www.scylladb.com/

.. toctree::
    :maxdepth: 1

    install
    usage