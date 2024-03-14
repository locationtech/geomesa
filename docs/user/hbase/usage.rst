Using the HBase Data Store Programmatically
===========================================

Creating a Data Store
---------------------

An instance of an HBase data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

The HBase data store also requires that an appropriate ``hbase-site.xml`` file is available on the classpath; the
connection parameters for HBase are obtained from this file, including ``hbase.zookeeper.quorum`` and
``hbase.zookeeper.property.clientPort``.

As an alternative to providing ``hbase-site.xml``, the Zookeeper connection can be specified through the
parameter ``hbase.zookeepers``. However, this method is not recommended, as other important configurations
(including security, if any) from ``hbase-site.xml`` may be required for correct operation.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("hbase.catalog", "geomesa");
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

.. _hbase_parameters:

HBase Data Store Parameters
---------------------------

The data store takes several parameters (required parameters are marked with ``*``):

===========================================  ======= ========================================================================================
Parameter                                    Type    Description
===========================================  ======= ========================================================================================
``hbase.catalog *``                          String  The name of the GeoMesa catalog table, including the HBase namespace (if any) separated
                                                     by a ``:``. For example, ``myCatalog`` or ``myNamespace:myCatalog``
``hbase.zookeepers``                         String  A comma-separated list of servers in the HBase zookeeper ensemble. This is optional,
                                                     the preferred method for defining the HBase connection is with ``hbase-site.xml``
``hbase.coprocessor.url``                    String  Path to the GeoMesa jar containing coprocessors, for auto registration
``hbase.config.paths``                       String  Additional HBase configuration resource files (comma-delimited)
``hbase.config.xml``                         String  Additional HBase configuration properties, as a standard XML ``<configuration>``
                                                     element
``hbase.connections.reuse``                  Boolean Re-use and share HBase connections, or create a new one for this data store
``hbase.remote.filtering``                   Boolean Can be used to disable remote filtering and coprocessors, for environments
                                                     where custom code can't be installed
``hbase.security.enabled``                   Boolean Enable HBase security (visibilities)
``geomesa.security.auths``                   String  Comma-delimited superset of authorizations that will be used for queries
``geomesa.security.force-empty-auths``       Boolean Forces authorizations to be empty
``geomesa.query.audit``                      Boolean Audit queries being run. Queries will be written to a log file
``geomesa.query.timeout``                    String  The max time a query will be allowed to run before being killed. The
                                                     timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.query.threads``                    Integer The number of threads to use per query
``hbase.coprocessor.threads``                Integer The number of HBase RPC threads to use per coprocessor query
``geomesa.query.loose-bounding-box``         Boolean Use loose bounding boxes - queries will be faster but may return extraneous results
``hbase.ranges.max-per-extended-scan``       Integer Max ranges per extended scan. Ranges will be grouped into scans based on this setting
``hbase.ranges.max-per-coprocessor-scan``    Integer Max ranges per coprocessor scan. Ranges will be grouped into scans based on this setting
``hbase.coprocessor.arrow.enable``           Boolean Disable coprocessor scans for Arrow queries, and use local encoding instead
``hbase.coprocessor.bin.enable``             Boolean Disable coprocessor scans for Bin queries, and use local encoding instead
``hbase.coprocessor.density.enable``         Boolean Disable coprocessor scans for density queries, and use local processing instead
``hbase.coprocessor.stats.enable``           Boolean Disable coprocessor scans for stat queries, and use local processing instead
``hbase.coprocessor.yield.partial.results``  Boolean Toggle coprocessors yielding partial results
``hbase.coprocessor.scan.parallel``          Boolean Toggle extremely parallel coprocessor scans (bounded by RPC threads)
``geomesa.stats.enable``                     Boolean Toggle collection of statistics (currently not implemented)
``geomesa.partition.scan.parallel``          Boolean For partitioned schemas, execute scans in parallel instead of sequentially
===========================================  ======= ========================================================================================

Note: the ``hbase.coprocessor.*.enable`` parameters will be superseded by ``hbase.remote.filtering=false``.

More information on using GeoTools can be found in the `GeoTools user guide
<https://docs.geotools.org/stable/userguide/>`__.
