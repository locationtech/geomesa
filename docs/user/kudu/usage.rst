Using the Kudu Data Store Programmatically
==========================================

Creating a Data Store
---------------------

An instance of a Kudu data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("kudu.master", "localhost");
    parameters.put("kudu.catalog", "geomesa");
    org.geotools.data.DataStore dataStore =
        org.geotools.data.DataStoreFinder.getDataStore(parameters);

.. _kudu_parameters:

Kudu Data Store Parameters
--------------------------

The data store takes several parameters (required parameters are marked with ``*``):

====================================== ======= ====================================================================================
Parameter                              Type    Description
====================================== ======= ====================================================================================
``kudu.master *``                      String  The host name of the Kudu master server
``kudu.catalog *``                     String  The name of the GeoMesa catalog table
``kudu.credentials``                   String  Kudu client authentication credentials
``kudu.worker.threads``                String  Number of worker threads used by the Kudu connection
``kudu.boss.threads``                  String  Number of boss threads used by the Kudu connection
``kudu.client.stats.disable``          Boolean Disable Kudu client statistics
``geomesa.security.auths``             String  Comma-delimited superset of authorizations that will be used for queries
``geomesa.query.audit``                Boolean Audit queries being run. Queries will be written to a log file
``geomesa.query.timeout``              String  The max time a query will be allowed to run before being killed. The
                                               timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.query.threads``              Integer The number of threads to use per query
``geomesa.query.loose-bounding-box``   Boolean Use loose bounding boxes - queries will be faster but may return extraneous results
``geomesa.stats.generate``             Boolean Toggle collection of statistics (currently not implemented)
``geomesa.query.caching``              Boolean Toggle caching of results
====================================== ======= ====================================================================================

More information on using GeoTools can be found in the `GeoTools user guide
<http://docs.geotools.org/stable/userguide/>`__.
