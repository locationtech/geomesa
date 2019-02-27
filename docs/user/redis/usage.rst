Using the Redis Data Store Programmatically
===========================================

Creating a Data Store
---------------------

An instance of a Redis data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("redis.url", "redis://localhost:6379");
    parameters.put("redis.catalog", "geomesa");
    org.geotools.data.DataStore dataStore =
        org.geotools.data.DataStoreFinder.getDataStore(parameters);

.. _redis_parameters:

Redis Data Store Parameters
---------------------------

The data store takes several parameters (required parameters are marked with ``*``):

====================================== ======= ====================================================================================
Parameter                              Type    Description
====================================== ======= ====================================================================================
``redis.url *``                        String  Redis connection URL. The URL can be used to specify the Redis database and
                                               credentials, if required - for example, 'redis://user:pass@host:port/db'
``redis.catalog *``                    String  The name of the GeoMesa catalog table. In Redis, this is the base key used for
                                               inserts
``redis.connection.pool.size``         Integer Max number of simultaneous connections to use
``redis.connection.pool.validate``     Boolean Test connections when borrowed from the pool. Connections may be closed due to
                                               inactivity, which would cause a transient error if validation is disabled
``redis.pipeline.enabled``             Boolean Enable pipelining of query requests. This reduces network latency, but restricts
                                               queries to a single execution thread
``geomesa.query.threads``              Integer The number of threads to use per query (if not pipelining)
``geomesa.query.timeout``              String  The max time a query will be allowed to run before being killed. The
                                               timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.stats.generate``             Boolean Toggle collection of statistics
``geomesa.query.audit``                Boolean Audit queries being run. Queries will be written to a log file
``geomesa.query.loose-bounding-box``   Boolean Use loose bounding boxes - queries will be faster but may return extraneous results
``geomesa.query.caching``              Boolean Toggle caching of results
``geomesa.security.auths``             String  Comma-delimited superset of authorizations that will be used for queries
``geomesa.security.force-empty-auths`` Boolean Forces authorizations to be empty
====================================== ======= ====================================================================================

More information on using GeoTools can be found in the `GeoTools user guide
<http://docs.geotools.org/stable/userguide/>`__.
