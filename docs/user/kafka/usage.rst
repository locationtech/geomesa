Using the Kafka Data Store Programmatically
===========================================

Creating a Data Store
---------------------

An instance of a Kafka data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath. To create a ``KafkaDataStore`` there are two
required properties, one for the Apache Kafka connection, ``kafka.brokers``, and one for the Apache
Zookeeper connection, ``kafka.zookeepers``. An optional parameter, ``kafka.zk.path`` is
used to specify a path in Zookeeper under which schemas are stored. If
no zk path is specified then a default path will be used. Configuration
parameters are described fully below.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("kafka.zookeepers", "localhost:2181");
    parameters.put("kafka.brokers", "localhost:9092");
    org.geotools.data.DataStore dataStore =
        org.geotools.data.DataStoreFinder.getDataStore(parameters);

.. _kafka_parameters:

Kafka Data Store Parameters
---------------------------

The Kafka Data Store takes several parameters (required parameters are marked with ``*``):

==================================== ======= ====================================================================================================
Parameter                            Type    Description
==================================== ======= ====================================================================================================
``kafka.brokers *``                  String  Kafka brokers, e.g. "localhost:9092"
``kafka.zookeepers *``               String  Kafka zookeepers, e.g "localhost:2181"
``kafka.zk.path``                    String  Zookeeper discoverable path (namespace)
``kafka.producer.config``            String  Configuration options for kafka producer, in Java properties
                                             format. See `Producer Configs <http://kafka.apache.org/documentation.html#producerconfigs>`_
``kafka.consumer.config``            String  Configuration options for kafka consumer, in Java properties
                                             format. See `New Consumer Configs <http://kafka.apache.org/documentation.html#newconsumerconfigs>`_
``kafka.consumer.from-beginning``    Boolean Start reading from the beginning of the topic (vs ignore existing messages). If enabled, features
                                             will not be available for query until all existing messages are processed. However, feature
                                             listeners will still be invoked as normal.
``kafka.consumer.count``             Integer Number of kafka consumers used per feature type. Set to 0
                                             to disable consuming (i.e. producer only)
``kafka.topic.partitions``           Integer Number of partitions to use in kafka topics
``kafka.topic.replication``          Integer Replication factor to use in kafka topics
``kafka.cache.expiry``               String  Expire features from in-memory cache after this delay, e.g. "10 minutes"
``kafka.cache.cleanup``              String  Clean expired cache entries every so often, e.g.
                                             "60 seconds". If not specified, expired features will be
                                             cleaned incrementally during reads and writes
``kafka.cache.consistency``          String  Check the feature cache for consistency at this interval, e.g. "1 hour"
``kafka.cache.cqengine``             Boolean Use CQEngine-based implementation of in-memory feature cache. See :ref:`in_memory_index` for details
``geomesa.query.loose-bounding-box`` Boolean Use loose bounding boxes, which offer improved performance but are not exact
``geomesa.query.audit``              Boolean Audit incoming queries. By default audits are written to a log file
``geomesa.security.auths``           String  Default authorizations used to query data, comma-separated
==================================== ======= ====================================================================================================

More information on using GeoTools can be found in the `GeoTools user guide
<http://docs.geotools.org/stable/userguide/>`__.
