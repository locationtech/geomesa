Using the Cassandra DataStore Programmatically
==============================================

An instance of a Cassandra data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("geomesa.cassandra.contact.point", "127.0.0.1:9142");
    parameters.put("geomesa.cassandra.keyspace", "geomesa");
    parameters.put("geomesa.cassandra.catalog.table", "mycatalog");
    org.geotools.data.DataStore dataStore =
        org.geotools.data.DataStoreFinder.getDataStore(parameters);

The data store requires three parameters:

* **geomesa.cassandra.contact.point** - the connection point for Cassandra, in the form ``<host>:<port>`` -
  for a default local installation this will be ``localhost:9042``.
* **geomesa.cassandra.keyspace** - the Cassandra keyspace to use (must exist already)
* **geomesa.cassandra.catalog.table** - the name of the Cassandra table that stores feature type data

In addition, there are several optional configuration parameters:

* **geomesa.cassandra.username** - Username used to connect to the Cassandra instance
* **geomesa.cassandra.password** - Password used to connect to the Cassandra instance
* **auditQueries** - Audit queries being run in a log file
* **caching** - Cache the results of queries for faster repeated searches. Warning: large result sets can swamp memory
* **looseBoundingBox** - use loose bounding boxes - queries will be faster but may return extraneous result
* **queryThreads** - the number of threads to use per query
* **queryTimeout** - the max time a query will be allowed to run before being killed, in seconds

More information on using GeoTools can be found in the `GeoTools user guide
<http://docs.geotools.org/stable/userguide/>`__.
