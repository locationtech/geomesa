Using ScyllaDB with GeoMesa
============================

ScyllaDB is a high-performance, drop-in replacement for Apache Cassandra that is fully
compatible with the Cassandra Query Language (CQL) and wire protocol.

Data Store Parameters and Usage
--------------------------------

ScyllaDB uses the **exact same parameters and API** as the Cassandra Data Store.
Please refer to the :ref:`cassandra_parameters` documentation for complete details on:

* Data store parameters
* Programmatic access
* Command line tools
* Configuration options

The only difference is that you point the ``cassandra.contact.point`` parameter to your
ScyllaDB cluster instead of a Cassandra cluster.

Quick Example
~~~~~~~~~~~~~

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("cassandra.contact.point", "127.0.0.1:9042");  // Your ScyllaDB cluster
    parameters.put("cassandra.keyspace", "geomesa_scylla");
    parameters.put("cassandra.catalog", "mycatalog");
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

ScyllaDB-Specific Notes
-----------------------

Performance
~~~~~~~~~~~

ScyllaDB typically offers superior performance compared to Cassandra:

* **Higher throughput**: Written in C++, handles more operations per second
* **Lower latency**: Better tail latencies, especially under load
* **Better resource utilization**: More efficient use of CPU and memory

Compatibility
~~~~~~~~~~~~~

* All Cassandra DataStore features work with ScyllaDB
* Same client drivers and CQL syntax
* ScyllaDB 2025.x or 6.x+ recommended
* No code changes required - true drop-in replacement

Migration from Cassandra
~~~~~~~~~~~~~~~~~~~~~~~~~

To migrate from Cassandra to ScyllaDB:

1. Export data: ``bin/geomesa-cassandra export``
2. Set up ScyllaDB with same keyspace structure
3. Import data: ``bin/geomesa-cassandra ingest``
4. Update connection string to point to ScyllaDB

Alternatively, use ScyllaDB's native migration tools to preserve all tables and data.
See the `ScyllaDB migration guide <https://opensource.docs.scylladb.com/stable/operating-scylla/procedures/cassandra-to-scylla-migration-process.html>`_.
