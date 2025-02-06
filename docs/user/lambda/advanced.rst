Advanced Topics
===============

Parallelism
-----------

There are two types of parallelism for the Lambda store - the number of simultaneous writers for long-term
persistence, and the number of Kafka consumers per data store.

The number of writers is determined by the number of data stores instances with a valid ``lambda.expiry`` parameter;
however the parallelism on writes is limited by how many partitions are in the Kafka topic (writes are
synchronized across instances by topic and partition). The ``lambda.kafka.partitions`` parameter can be used to control
the number of partitions, but note that once a topic is created, you will need to use Kafka scripts (i.e.
``kafka-topics.sh``) to modify partitions.

The number of consumers used to load features into the in-memory cache is controlled by the ``lambda.kafka.consumers``
parameter. This is the number of consumers per data store instance per simple feature type accessed. Note that
having more consumers than topic partitions is not recommended and will cause some consumers to be idle.

Installation Tips
-----------------

The typical use case for a Lambda data store is to be fed by an analytic streaming process. The analytic will
update summary ``SimpleFeature``\ s with derived attributes containing analytic results. For example, when
aggregating GPS tracks, there could be a dynamically updated field for current estimated heading.

Typical streaming analytics will run in many concurrent threads. The Lambda
store achieves parallelism based on the ``lambda.kafka.partitions`` parameter. Generally you should start with this
set to the number of writer threads being used, and adjust up or down as needed.

Any data store instances used only for reads (e.g. GeoServer) should generally disable writing by setting the
``lambda.persist`` parameter to ``false``. Note that there must be at least one data store instance with persistence enabled,
or features will never be removed from memory and persisted to long-term storage.

Monitoring State
----------------

The state of a Lambda data store can be monitored by enabling logging on the following classes:

================================================================= ========= ===============================================
Class                                                             Level     Info
================================================================= ========= ===============================================
org.locationtech.geomesa.lambda.stream.kafka.KafkaStore           ``trace`` All features written to Kafka
org.locationtech.geomesa.lambda.stream.kafka.KafkaCacheLoader     ``trace`` All features read from Kafka
org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache    ``debug`` Partition assignments, Size of in-memory cache,
                                                                            summary of writes to persistent storage
org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache    ``trace`` All features added/removed from in-memory cache
                                                                            or written to persistent storage
================================================================= ========= ===============================================
