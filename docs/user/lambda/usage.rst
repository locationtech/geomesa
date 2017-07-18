Using the Lambda Data Store Programmatically
============================================

Creating a Data Store
---------------------

An instance of a Lambda data store can be obtained through the normal GeoTools discovery methods, assuming
that the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, String> parameters = new HashMap<>;
    parameters.put("accumulo.instanceId", "myInstance");
    parameters.put("accumulo.zookeepers", "zoo1,zoo2,zoo3");
    parameters.put("accumulo.user", "myUser");
    parameters.put("accumulo.password", "myPassword");
    parameters.put("accumulo.tableName", "my_table");
    parameters.put("kafka.brokers", "kafka1:9092,kafka2:9092");
    parameters.put("kafka.zookeepers", "zoo1,zoo2,zoo3");
    parameters.put("expiry", "10 minutes");
    org.geotools.data.DataStore dataStore = org.geotools.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <http://docs.geotools.org/stable/userguide/>`_.

.. _lambda_parameters:

Parameters
----------

The Lambda Data Store takes several parameters:

====================== =============================================================================================================================================================================================================
Parameter              Description
====================== =============================================================================================================================================================================================================
accumulo.instanceId *  The instance ID of the Accumulo installation
accumulo.zookeepers *  A comma separated list of zookeeper servers (e.g. "zoo1,zoo2,zoo3" or "localhost:2181")
accumulo.tableName *   The name of the GeoMesa catalog table
accumulo.user *        Accumulo username
accumulo.password      Accumulo password
accumulo.keytabPath    Path to a Kerberos keytab file containing an entry for the specified user
kafka.brokers *        A comma separated list of kafka brokers (e.g. ``broker1:9092,broker2:9092``)
kafka.zookeepers *     A comma separated list of zookeeper servers (e.g. ``zoo1,zoo2,zoo3`` or ``localhost:2181``)
expiry *               A duration for how long features are kept in memory before being persisted (e.g. ``10 minutes``). Using ``Inf`` will cause the data store to not participate in persisting expired entries
persist                Whether expired features should be persisted to Accumulo or just discarded
accumulo.queryTimeout  The max time (in sec) a query will be allowed to run before being killed
accumulo.queryThreads  The number of threads to use per query
accumulo.recordThreads The number of threads to use for record retrieval
accumulo.writeThreads  The number of threads to use for writing records
kafka.partitions       Number of partitions used to create new topics. You should generally set this to the number of writer instances you plan to run
kafka.consumers        Number of consumers used to load data into the in-memory cache
kafka.producer.options Java-properties-formatted string that is passed directly to the Kafka producer. See `Producer Configs <http://kafka.apache.org/090/documentation.html#producerconfigs>`_
kafka.consumer.options Java-properties-formatted string that is passed directly to the Kafka consumer. See `New Consumer Configs <http://kafka.apache.org/090/documentation.html#newconsumerconfigs>`_
visibilities           Visibility labels to apply to all written data
auths                  Comma-delimited superset of authorizations that will be used for filtering data
looseBoundingBox       Use non-exact matching for bounding box filters, which will speed up queries
collectStats           Enable collection of data statistics
auditQueries           Enable auditing of queries against the data store
====================== =============================================================================================================================================================================================================

The required parameters are marked with an asterisk. One (but not both) of ``accumulo.password`` and
``accumulo.keytabPath`` must be provided.
