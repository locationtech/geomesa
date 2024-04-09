Using the Partitioned PostGIS Data Store Programmatically
=========================================================

Creating a Data Store
---------------------

An instance of a PostGIS data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

.. code-block:: java

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("dbtype", "postgis-partitioned");
    parameters.put("host", "localhost");
    parameters.put("port", "5432");
    parameters.put("database", "geomesa");
    parameters.put("user", "postgres");
    parameters.put("passwd", "postgres");
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

.. _pg_partition_parameters:

Partitioned PostGIS Data Store Parameters
-----------------------------------------

The partitioned PostGIS data store uses the same parameters as the standard PostGIS data store, except
that ``dbtype`` **must** be set to ``postgis-partitioned``. See the
`JDBCDataStore <https://docs.geotools.org/stable/userguide/library/jdbc/datastore.html>`__ and
`PostGIS Plugin <https://docs.geotools.org/stable/userguide/library/jdbc/postgis.html>`__ for other parameters.

The following additional parameters are also supported:

======================================= ======== ===================================================================================================================================
Parameter                               Type     Description
======================================= ======== ===================================================================================================================================
``idle_in_transaction_session_timeout`` Duration Transaction idle timeout (e.g. ``2 minutes``). See the
                                                 `Postgres documentation <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT>`__
                                                 for more information. Setting this timeout may help prevent
                                                 abandoned queries from slowing down database operations.
======================================= ======== ===================================================================================================================================
