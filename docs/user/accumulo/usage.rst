Using the Accumulo Data Store Programmatically
==============================================

Creating a Data Store
---------------------

An instance of an Accumulo data store can be obtained through the normal GeoTools discovery methods, assuming
that the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, String> parameters = new HashMap<>;
    parameters.put("accumulo.instance.name", "myInstance");
    parameters.put("accumulo.zookeepers", "myZoo1,myZoo2,myZoo3");
    parameters.put("accumulo.user", "myUser");
    parameters.put("accumulo.password", "myPassword");
    parameters.put("accumulo.catalog", "myNamespace.myTable");
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

Instead of specifying the cluster connection explicitly, an appropriate ``accumulo-client.properties`` may be added
to the classpath. See the
`Accumulo documentation <https://accumulo.apache.org/docs/2.x/getting-started/clients#creating-an-accumulo-client>`_
for information on the necessary configuration keys. Any explicit data store parameters will take precedence over
the configuration file.

More information on using GeoTools can be found in the `GeoTools user guide <https://docs.geotools.org/stable/userguide/>`_.

.. _accumulo_parameters:

Accumulo Data Store Parameters
------------------------------

The Accumulo Data Store takes several parameters (required parameters are marked with ``*``):

====================================== ======= ==========================================================================
Parameter                              Type    Description
====================================== ======= ==========================================================================
``accumulo.catalog *``                 String  The name of the GeoMesa catalog table, including the Accumulo namespace
                                               (e.g. "myNamespace.myCatalog")
``accumulo.instance.name``             String  The name of the Accumulo instance
``accumulo.zookeepers``                String  A comma separated list of zookeeper servers (e.g. "zoo1,zoo2,zoo3"
                                               or "localhost:2181")
``accumulo.user``                      String  The username used to connect to Accumulo
``accumulo.password``                  String  The password for the Accumulo user
``accumulo.keytab.path``               String  Path to a Kerberos keytab file containing an entry for the specified user
``geomesa.security.auths``             String  Comma-delimited superset of authorizations that will be used for
                                               queries via Accumulo
``geomesa.security.force-empty-auths`` Boolean Forces authorizations to be empty
``geomesa.security.auth-provider``     String  Class name for an ``AuthorizationsProvider`` implementation
``geomesa.query.audit``                Boolean Audit queries being run. Queries will be stored in a
                                               ``<catalog>_queries`` table
``geomesa.query.timeout``              String  The max time a query will be allowed to run before being killed. The
                                               timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.query.threads``              Integer The number of threads to use per query
``geomesa.query.loose-bounding-box``   Boolean Use loose bounding boxes - queries will be faster but may return
                                               extraneous results
``accumulo.query.record-threads``      Integer The number of threads to use for record retrieval
``accumulo.write.threads``             Integer The number of threads to use for writing records
``geomesa.stats.enable``               Boolean Toggle collection of statistics for newly created feature types
``accumulo.remote.arrow.enable``       Boolean Process Arrow encoding in Accumulo tablets servers as a
                                               distributed call
``accumulo.remote.bin.enable``         Boolean Process binary encoding in Accumulo tablets servers as a
                                               distributed call
``accumulo.remote.density.enable``     Boolean Process heatmap encoding in Accumulo tablets servers as a
                                               distributed call
``accumulo.remote.stats.enable``       Boolean Process statistical calculations in Accumulo tablets servers as a
                                               distributed call
``geomesa.partition.scan.parallel``    Boolean For partitioned schemas, execute scans in parallel instead of sequentially
====================================== ======= ==========================================================================

Note: it is an error to specify both ``accumulo.password`` and ``accumulo.keytab.path``.
