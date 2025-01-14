Cassandra / ScyllaDB Data Store
====================

.. note::

    GeoMesa currently supports Cassandra version |cassandra_version|.

.. note::

    GeoMesa currently supports ScyllaDB version |scylladb_version|.

The GeoMesa Cassandra Data Store is an implementation of the GeoTools
``DataStore`` interface that is backed by `Apache Cassandra`_ or `ScyllaDB`.
It is found in the ``geomesa-cassandra`` directory of the GeoMesa
source distribution. As `ScyllaDB` is API-compatible with `Apache Cassandra`_,
it is also supported and work with the same configuration
just by pointing to `ScyllaDB`_ in connection string.

.. _Apache Cassandra: https://cassandra.apache.org/

.. _ScyllaDB: https://www.scylladb.com/

.. toctree::
    :maxdepth: 1

    install
    usage
    geoserver
    commandline
    configuration
    heatmaps
