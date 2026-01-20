.. _kafka_parameters:

Kafka Data Store Parameters
===========================

The Kafka data store differs from most data stores in that the data set is kept entirely in memory. Because of this,
the in-memory indexing can be configured at runtime through data store parameters. See :ref:`kafka_index_config` for
more information on the available indexing options.

Because configuration options can reference attributes from a particular SimpleFeatureType, it may be necessary to
create multiple Kafka data store instances when dealing with multiple schemas.

Use the following parameters for a Kafka data store (required parameters are marked with ``*``):

============================================ ======= ====================================================================================================
Parameter                                    Type    Description
============================================ ======= ====================================================================================================
``kafka.brokers *``                          String  Kafka bootstrap servers, e.g. ``localhost:9092``
``kafka.catalog.topic``                      String  The Kafka topic used to store schema metadata, defaults to ``geomesa-catalog``
``kafka.producer.config``                    String  Configuration options for kafka producer, in Java properties
                                                     format. See `Producer Configs <https://kafka.apache.org/documentation.html#producerconfigs>`_
``kafka.producer.clear``                     Boolean Send a 'clear' message on startup. This will cause clients to drop any data that was in the
                                                     topic prior to startup
``kafka.consumer.config``                    String  Configuration options for kafka consumer, in Java properties
                                                     format. See `Consumer Configs <https://kafka.apache.org/documentation.html#consumerconfigs>`_
``kafka.consumer.read-back``                 String  On start up, read messages that were written within this time frame (vs ignore old messages), e.g.
                                                     ``1 hour``. Use ``Inf`` to read all messages. If enabled, features will not be available for query
                                                     until all existing messages are processed. However, feature listeners will still be invoked as
                                                     normal. See :ref:`kafka_initial_load`
``kafka.consumer.count``                     Integer Number of kafka consumers used per feature type. Set to 0 to disable consuming (i.e. producer mode)
``kafka.consumer.offset-commit-interval``    String  How often to commit offsets for the consumer group, by default ``10 seconds``
``kafka.consumer.group-prefix``              String  Prefix to use for kafka group ID, to more easily identify particular data stores
``kafka.consumer.start-on-demand``           Boolean Start consuming from a topic only when the feature type is first accessed, defaults to ``true``.
                                                     This can reduce load and memory overhead if some layers are never queried, however it may cause a
                                                     layer to initially appear empty, until the consumers have had time to spin up
``kafka.topic.partitions``                   Integer Number of partitions to use when creating new kafka topics
``kafka.topic.replication``                  Integer Replication factor to use when creating new kafka topics
``kafka.topic.truncate-on-delete``           Boolean Instead of deleting the Kafka topic when a schema is deleted, mark all messages on the topic as
                                                     deleted but preserve the topic
``kafka.serialization.type``                 String  Internal serialization format to use for kafka messages. Must be one of ``kryo``, ``avro``
                                                     or ``avro-native``. See :ref:`kafka_serialization_format` for additional details
``kafka.cache.expiry``                       String  Expire features from in-memory cache after this delay, e.g. ``10 minutes``. See :ref:`kafka_expiry`
``kafka.cache.expiry.dynamic``               String  Expire features dynamically based on CQL predicates. See :ref:`kafka_expiry`
``kafka.cache.event-time``                   String  Instead of message time, determine expiry based on feature data. See :ref:`kafka_event_time`
``kafka.cache.event-time.ordering``          Boolean Instead of message time, determine feature ordering based on the feature event time.
                                                     See :ref:`kafka_event_time`
``kafka.index.cqengine``                     String  Use CQEngine-based attribute indices for the in-memory feature cache. See :ref:`kafka_cqengine`
``kafka.index.resolution.x``                 Integer Number of bins in the x-dimension of the spatial index, by default 360. See
                                                     :ref:`kafka_index_resolution`
``kafka.index.resolution.y``                 Integer Number of bins in the y-dimension of the spatial index, by default 180. See
                                                     :ref:`kafka_index_resolution`
``kafka.index.tiers``                        String  Number and size of tiers used for indexing geometries with extents, in the form ``x1:y1,x2:y2``.
                                                     See :ref:`kafka_ssi`
``kafka.serialization.lazy``                 Boolean Use lazy deserialization of features. This may improve processing load at
                                                     the expense of slightly slower query times
``kafka.layer.views``                        String  Additional views on existing schemas to expose as layers. See :ref:`kafka_layer_views` for details
``geomesa.metrics.registry``                 String  Specify the type of registry used to publish metrics. Must be one of ``none``,
                                                     ``prometheus``, or ``cloudwatch``. See :ref:`geomesa_metrics` for registry details.
``geomesa.metrics.registry.config``          String  Override the default registry config. See :ref:`geomesa_metrics` for configuration details.
``geomesa.query.loose-bounding-box``         Boolean Use loose bounding boxes, which offer improved performance but are not exact
``geomesa.query.audit``                      Boolean Audit incoming queries. By default audits are written to a log file
``geomesa.security.auths``                   String  Comma-delimited superset of authorizations that will be used for queries. See
                                                     :ref:`reading_vis_labels` for details
``geomesa.security.auth-provider``           String  Class name for an ``AuthorizationsProvider`` implementation
============================================ ======= ====================================================================================================

.. _kafka_parameters_zk:

Zookeeper (deprecated)
----------------------

Historically, the Kafka data store persisted schema information in Zookeeper. However, since Kafka has deprecated (in 3.x) and
then removed (in 4.x) support for Zookeeper, GeoMesa now defaults to storing schema information in Kafka itself.

For existing schemas that are persisted in Zookeeper, the following deprecated parameters can be used:

==================== ======= ====================================================================================================
Parameter            Type    Description
==================== ======= ====================================================================================================
``kafka.zookeepers`` String  Comma-delimited list of Zookeeper URLs, e.g ``localhost:2181``, used to persist GeoMesa metadata
                             in Zookeeper instead of in Kafka topics
``kafka.zk.path``    String  Zookeeper discoverable path, used to namespace feature types
==================== ======= ====================================================================================================

See :ref:`no_zookeeper` for details on migrating away from Zookeeper.

Programmatic Access
-------------------

An instance of a Kafka data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath.

.. code-block:: java

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("kafka.brokers", "localhost:9092");
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <https://docs.geotools.org/stable/userguide/>`_.
