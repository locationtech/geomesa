Using the Cassandra DataStore Programmatically
==============================================

Creating a Data Store
---------------------

An instance of a Cassandra data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("cassandra.contact.point", "127.0.0.1:9142");
    parameters.put("cassandra.keyspace", "geomesa");
    parameters.put("cassandra.catalog", "mycatalog");
    org.geotools.data.DataStore dataStore =
        org.geotools.data.DataStoreFinder.getDataStore(parameters);

.. _cassandra_parameters:

Cassandra Data Store Parameters
-------------------------------

The data store takes several parameters (required parameters are marked with ``*``):

==================================== ======= ========================================================================================
Parameter                            Type    Description
==================================== ======= ========================================================================================
``cassandra.catalog *``              String  The name of the GeoMesa catalog table (previously ``geomesa.cassandra.catalog.table``)
``cassandra.contact.point *``        String  The connection point for Cassandra, in the form ``<host>:<port>`` - for a default
                                             local installation this will be ``localhost:9042``
``cassandra.keyspace *``             String  The Cassandra keyspace to use (must exist already)
``cassandra.username``               String  Cassandra user
``cassandra.password``               String  Cassandra password
``geomesa.query.audit``              Boolean Audit queries being run. Queries will be written to a log file
``geomesa.query.timeout``            String  The max time a query will be allowed to run before being killed. The
                                             timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.query.threads``            Integer The number of threads to use per query
``geomesa.query.loose-bounding-box`` Boolean Use loose bounding boxes - queries will be faster but may return extraneous results
``geomesa.stats.generate``           Boolean Toggle collection of statistics (currently not implemented)
``geomesa.query.caching``            Boolean Toggle caching of results
==================================== ======= ========================================================================================

More information on using GeoTools can be found in the `GeoTools user guide
<http://docs.geotools.org/stable/userguide/>`__.
