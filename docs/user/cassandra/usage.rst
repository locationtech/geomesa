.. _cassandra_parameters:

Cassandra Data Store Parameters
===============================

Use the following parameters for a Cassandra data store (required parameters are marked with ``*``):

==================================== ======= ========================================================================================
Parameter                            Type    Description
==================================== ======= ========================================================================================
``cassandra.catalog *``              String  The name of the GeoMesa catalog table (previously ``geomesa.cassandra.catalog.table``)
``cassandra.contact.point *``        String  The connection point for Cassandra, in the form ``<host>:<port>`` - for a default
                                             local installation this will be ``localhost:9042``
``cassandra.local-datacenter *``     String  The Cassandra datacenter to prioritize for queries and load balancing
``cassandra.keyspace *``             String  The Cassandra keyspace to use (must exist already)
``cassandra.username``               String  Cassandra user
``cassandra.password``               String  Cassandra password
``cassandra.query.consistency``      String  The consistency level for reads (queries and metadata). Must be one of ``ONE``,
                                             ``TWO``, ``THREE``, ``QUORUM``, ``ALL``, ``LOCAL_ONE``, ``LOCAL_QUORUM`` or
                                             ``EACH_QUORUM``. Writes are not affected. If not specified, the driver default is
                                             used, see :doc:`/user/cassandra/configuration`
``geomesa.query.audit``              Boolean Audit queries being run. Queries will be written to a log file
``geomesa.query.timeout``            String  The max time a query will be allowed to run before being killed. The
                                             timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.query.threads``            Integer The number of threads to use per query
``geomesa.query.loose-bounding-box`` Boolean Use loose bounding boxes - queries will be faster but may return extraneous results
``geomesa.metrics.registry``         String  Specify the type of registry used to publish metrics. Must be one of ``none``,
                                             ``prometheus``, or ``cloudwatch``. See :ref:`geomesa_metrics` for registry details.
``geomesa.metrics.registry.config``  String  Override the default registry config. See :ref:`geomesa_metrics` for configuration
                                             details.
``geomesa.stats.enable``             Boolean Toggle collection of statistics (currently not implemented)
==================================== ======= ========================================================================================

Specify the keyspace separately from the catalog name. For example, use ``-k abc -c foobar``
with the command-line tools, or ``cassandra.keyspace=abc`` and ``cassandra.catalog=foobar``
in data store parameters. Catalog names must not contain a dot.

Programmatic Access
-------------------

An instance of a Cassandra data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("cassandra.contact.point", "127.0.0.1:9142");
    parameters.put("cassandra.keyspace", "geomesa");
    parameters.put("cassandra.catalog", "mycatalog");
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <https://docs.geotools.org/stable/userguide/>`_.
