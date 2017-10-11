Using the Accumulo Data Store Programmatically
==============================================

Creating a Data Store
---------------------

An instance of an Accumulo data store can be obtained through the normal GeoTools discovery methods, assuming that the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, String> parameters = new HashMap<>;
    parameters.put("accumulo.instance.id", "myInstance");
    parameters.put("accumulo.zookeepers", "zoo1,zoo2,zoo3");
    parameters.put("accumulo.user", "myUser");
    parameters.put("accumulo.password", "myPassword");
    parameters.put("accumulo.catalog", "my_table");
    org.geotools.data.DataStore dataStore = org.geotools.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <http://docs.geotools.org/stable/userguide/>`_.

.. _accumulo_parameters:

Accumulo Data Store Parameters
------------------------------

The Accumulo Data Store takes several parameters (required parameters are marked with ``*``):

====================================== ======= ==========================================================================
Parameter                              Type    Description
====================================== ======= ==========================================================================
``accumulo.instance.id *``             String  The ID of the Accumulo instance
``accumulo.zookeepers *``              String  A comma separated list of zookeeper servers (e.g. "zoo1,zoo2,zoo3"
                                               or "localhost:2181")
``accumulo.user *``                    String  Accumulo username
``accumulo.password``                  String  Accumulo password
``accumulo.keytab.path``               String  Path to a Kerberos keytab file containing an entry for the specified user
``accumulo.catalog *``                 String  The name of the GeoMesa catalog table, including Accumulo namespace,
                                               if any (previously ``tableName``)
``geomesa.security.auths``             String  Comma-delimited superset of authorizations that will be used for
                                               queries via Accumulo
``geomesa.security.force-empty-auths`` Boolean Forces authorizations to be empty
``geomesa.security.auth-provider``     String  Class name for an ``AuthorizationsProvider`` implementation
``geomesa.security.visibilities``      String  Visibilities to apply to all written data
``geomesa.query.audit``                Boolean Audit queries being run. Queries will be stored in a
                                               ``<catalog>_queries`` table
``geomesa.query.timeout``              String  The max time a query will be allowed to run before being killed. The
                                               timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.query.threads``              Integer The number of threads to use per query
``geomesa.query.loose-bounding-box``   Boolean Use loose bounding boxes - queries will be faster but may return
                                               extraneous results
``accumulo.query.record-threads``      Integer The number of threads to use for record retrieval
``accumulo.write.threads``             Integer The number of threads to use for writing records
``geomesa.stats.generate``             Boolean Toggle collection of statistics
``geomesa.query.caching``              Boolean Toggle caching of results
``accumulo.mock``                      Boolean Use a mock connection (for testing)
====================================== ======= ==========================================================================

Note: one (but not both) of ``accumulo.password`` and ``accumulo.keytab.path`` must be provided.
