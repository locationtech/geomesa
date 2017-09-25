Using the Kafka Data Store Programmatically
===========================================

To create a ``KafkaDataStore`` there are two required properties, one
for the Apache Kafka connection, ``kafka.brokers``, and one for the Apache
Zookeeper connection, ``kafka.zookeepers``. An optional parameter, ``kafka.zk.path`` is
used to specify a path in Zookeeper under which schemas are stored. If
no zk path is specified then a default path will be used. Configuration
parameters are described fully below.

.. _kafka_parameters:

Parameters
----------

The Kafka Data Store takes several parameters (required parameters are marked with a ``*``):

================================= ======= ================ ===================================================================================================
Parameter                         Type    Default          Description
================================= ======= ================ ===================================================================================================
``kafka.brokers*``                String                   Kafka brokers, e.g. "localhost:9092"
``kafka.zookeepers*``             String                   Kafka zookeepers, e.g "localhost:2181"
``kafka.zk.path``                 String  geomesa/ds/kafka Zookeeper discoverable path (namespace)
``kafka.producer.config``         String                   Configuration options for kafka producer, in Java properties
                                                           format. See `Producer Configs <http://kafka.apache.org/documentation.html#producerconfigs>`_
``kafka.consumer.config``         String                   Configuration options for kafka consumer, in Java properties
                                                           format. See `New Consumer Configs <http://kafka.apache.org/documentation.html#newconsumerconfigs>`_
``kafka.consumer.from-beginning`` Boolean false            Start reading from the beginning of the topic (vs ignore
                                                           old messages)
``kafka.consumer.count``          Integer 1                Number of kafka consumers used per feature type. Set to 0
                                                           to disable consuming (i.e. producer only)
``kafka.topic.partitions``        Integer 1                Number of partitions to use in kafka topics
``kafka.topic.replication``       Integer 1                Replication factor to use in kafka topics
``kafka.cache.expiry``            String                   Expire features from in-memory cache after this delay,
                                                           e.g. "10 minutes"
``kafka.cache.cleanup``           String                   Clean expired cache entries every so often, e.g.
                                                           "60 seconds". If not specified, expired features will be
                                                           cleaned incrementally during reads and writes
``kafka.cache.cqengine``          Boolean                  Use CQEngine-based implementation of in-memory feature cache
``looseBoundingBox``              Boolean true             Use loose bounding boxes, which offer improved performance
                                                           but are not exact
``auditQueries``                  Boolean true             Audit incoming queries. By default audits are written to a log file
``auths``                         String                   Default authorizations used to query data, comma-separated
================================= ======= ================ ===================================================================================================
