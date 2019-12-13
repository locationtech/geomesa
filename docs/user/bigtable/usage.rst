Using the Bigtable Data Store Programmatically
==============================================

Creating a Data Store
---------------------

An instance of a Bigtable data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath. The Bigtable data store also requires that an
``hbase-site.xml`` be located on the classpath; the connection parameters for the Bigtable data store
are obtained from this file. For more information, see `Connecting to Cloud Bigtable
<https://cloud.google.com/bigtable/docs/connecting-hbase>`__.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>;
    parameters.put("bigtable.catalog", "geomesa");
    org.geotools.data.DataStore dataStore =
        org.geotools.data.DataStoreFinder.getDataStore(parameters);

.. _bigtable_parameters:

Bigtable Data Store Parameters
------------------------------

The data store takes several parameters (required parameters are marked with ``*``):

==================================== ======= ====================================================================================
Parameter                            Type    Description
==================================== ======= ====================================================================================
``bigtable.catalog *``               String  The name of the GeoMesa catalog table (previously ``bigtable.table.name``)
``geomesa.query.audit``              Boolean Audit queries being run. Queries will be written to a log file
``geomesa.query.timeout``            String  The max time a query will be allowed to run before being killed. The
                                             timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.query.threads``            Integer The number of threads to use per query
``geomesa.query.loose-bounding-box`` Boolean Use loose bounding boxes - queries will be faster but may return extraneous results
``geomesa.stats.generate``           Boolean Toggle collection of statistics (currently not implemented)
``geomesa.query.caching``            Boolean Toggle caching of results
==================================== ======= ====================================================================================

More information on using GeoTools can be found in the `GeoTools user guide
<http://docs.geotools.org/stable/userguide/>`__.
