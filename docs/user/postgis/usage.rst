Using the Partitioned PostGIS Data Store Programmatically
=========================================================

Creating a Data Store
---------------------

An instance of a PostGIS data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("dbtype", "postgis-partitioned");
    parameters.put("host", "localhost");
    parameters.put("database", "geomesa");
    parameters.put("user", "postgres");
    parameters.put("passwd", "postgres");
    org.geotools.data.DataStore dataStore =
        org.geotools.data.DataStoreFinder.getDataStore(parameters);

.. _pg_partition_parameters:

Partitioned PostGIS Data Store Parameters
-----------------------------------------

The partitioned PostGIS data store uses the same parameters as the standard PostGIS data store, except
that ``dbtype`` **must** be set to ``postgis-partitioned``. See the
`JDBCDataStore <https://docs.geotools.org/stable/userguide/library/jdbc/datastore.html>`__ and
`PostGIS Plugin <https://docs.geotools.org/stable/userguide/library/jdbc/postgis.html>`__ for other parameters.

Note that once a schema has been created, the regular PostGIS data store may be used for query, insert, update
and delete operations.
