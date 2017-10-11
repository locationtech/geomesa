Using the Lambda Data Store Programmatically
============================================

Creating a Data Store
---------------------

An instance of a Lambda data store can be obtained through the normal GeoTools discovery methods, assuming
that the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, String> parameters = new HashMap<>;
    parameters.put("lambda.accumulo.instance.id", "myInstance");
    parameters.put("lambda.accumulo.zookeepers", "zoo1,zoo2,zoo3");
    parameters.put("lambda.accumulo.user", "myUser");
    parameters.put("lambda.accumulo.password", "myPassword");
    parameters.put("lambda.accumulo.tableName", "my_table");
    parameters.put("lambda.kafka.brokers", "kafka1:9092,kafka2:9092");
    parameters.put("lambda.kafka.zookeepers", "zoo1,zoo2,zoo3");
    parameters.put("lambda.expiry", "10 minutes");
    org.geotools.data.DataStore dataStore = org.geotools.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <http://docs.geotools.org/stable/userguide/>`_.

.. _lambda_parameters:

Lambda Data Store Parameters
----------------------------

The data store takes several parameters (required parameters are marked with ``*``):

====================================== ======= ==================================================================================================
Parameter                              Type    Description
====================================== ======= ==================================================================================================
``lambda.accumulo.instance.id *``      String  The instance ID of the Accumulo installation
``lambda.accumulo.zookeepers *``       String  A comma separated list of zookeeper servers (e.g. "zoo1,zoo2,zoo3" or "localhost:2181")
``lambda.accumulo.catalog *``          String  The name of the GeoMesa catalog table
``lambda.accumulo.user *``             String  Accumulo username
``lambda.accumulo.password``           String  Accumulo password
``lambda.accumulo.keytab.path``        String  Path to a Kerberos keytab file containing an entry for the specified user
``lambda.accumulo.mock``               Boolean Use a mock connection (for testing)
``lambda.kafka.brokers *``             String  A comma separated list of kafka brokers (e.g. ``broker1:9092,broker2:9092``)
``lambda.kafka.zookeepers *``          String  A comma separated list of zookeeper servers (e.g. ``zoo1,zoo2,zoo3`` or ``localhost:2181``)
``lambda.kafka.partitions``            Integer Number of partitions used to create new topics. You should generally set this to the number of
                                               writer instances you plan to run
``lambda.kafka.consumers``             Integer Number of consumers used to load data into the in-memory cache
``lambda.kafka.producer.options``      String  Java-properties-formatted string that is passed directly to the Kafka producer.
                                               See `Producer Configs <http://kafka.apache.org/090/documentation.html#producerconfigs>`_
``lambda.kafka.consumer.options``      String  Java-properties-formatted string that is passed directly to the Kafka consumer.
                                               See `New Consumer Configs <http://kafka.apache.org/090/documentation.html#newconsumerconfigs>`_
``lambda.expiry *``                    String  A duration for how long features are kept in memory before being persisted (e.g. ``10 minutes``).
                                               Using ``Inf`` will cause the data store to not participate in persisting expired entries
``lambda.persist``                     Boolean Whether expired features should be persisted to Accumulo or just discarded
``geomesa.security.auths``             String  Comma-delimited superset of authorizations that will be used for queries via Accumulo
``geomesa.security.force-empty-auths`` Boolean Forces authorizations to be empty
``geomesa.security.auth-provider``     String  Class name for an ``AuthorizationsProvider`` implementation
``geomesa.security.visibilities``      String  Visibilities to apply to all written data
``geomesa.query.audit``                Boolean Audit queries being run. Queries will be stored in a ``<catalog>_queries`` table
``geomesa.query.timeout``              String  The max time a query will be allowed to run before being killed. The
                                               timeout is specified as a duration, e.g. ``1 minute`` or ``60 seconds``
``geomesa.query.threads``              Integer The number of threads to use per query
``geomesa.query.loose-bounding-box``   Boolean Use loose bounding boxes - queries will be faster but may return extraneous results
``accumulo.query.record-threads``      Integer The number of threads to use for record retrieval
``accumulo.write.threads``             Integer The number of threads to use for writing records
``geomesa.stats.generate``             Boolean Toggle collection of statistics
``geomesa.query.caching``              Boolean Toggle caching of results
====================================== ======= ==================================================================================================

Note: one (but not both) of ``lambda.accumulo.password`` and ``lambda.accumulo.keytab.path`` must be provided.
