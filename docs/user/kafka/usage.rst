Using the Kafka Data Store Programmatically
===========================================

Creating a Data Store
---------------------

An instance of a Kafka data store can be obtained through the normal GeoTools discovery methods,
assuming that the GeoMesa code is on the classpath. To create a ``KafkaDataStore`` there are two
required properties, one for the Apache Kafka connection, ``kafka.brokers``, and one for the Apache
Zookeeper connection, ``kafka.zookeepers``. An optional parameter, ``kafka.zk.path`` is
used to specify a path in Zookeeper under which schemas are stored. If no zk path is specified then
a default path will be used. Configuration parameters are described fully below.

.. code-block:: java

    import org.geotools.data.DataStore;
    import org.geotools.data.DataStoreFinder;

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("kafka.zookeepers", "localhost:2181");
    parameters.put("kafka.brokers", "localhost:9092");
    DataStore dataStore = DataStoreFinder.getDataStore(parameters);

.. _kafka_parameters:

Kafka Data Store Parameters
---------------------------

The Kafka data store differs from most data stores in that the data set is kept entirely in memory. Because of this,
the in-memory indexing can be configured at runtime through data store parameters. See :ref:`kafka_index_config` for
more information on the available indexing options.

Because configuration options can reference attributes from a particular SimpleFeatureType, it may be necessary to
create multiple Kafka data store instances when dealing with multiple schemas.

The Kafka data store accepts the following parameters (required parameters are marked with ``*``):

==================================== ======= ====================================================================================================
Parameter                            Type    Description
==================================== ======= ====================================================================================================
``kafka.brokers *``                  String  Kafka brokers, e.g. "localhost:9092"
``kafka.zookeepers *``               String  Kafka zookeepers, e.g "localhost:2181"
``kafka.zk.path``                    String  Zookeeper discoverable path, can be used to effectively namespace feature types
``kafka.producer.config``            String  Configuration options for kafka producer, in Java properties
                                             format. See `Producer Configs <http://kafka.apache.org/documentation.html#producerconfigs>`_
``kafka.producer.clear``             Boolean Send a 'clear' message on startup. This will cause clients to ignore any data that was in the
                                             topic prior to startup
``kafka.consumer.config``            String  Configuration options for kafka consumer, in Java properties
                                             format. See `New Consumer Configs <http://kafka.apache.org/documentation.html#newconsumerconfigs>`_
``kafka.consumer.read-back``         String  On start up, read messages that were written within this time frame (vs ignore old messages), e.g.
                                             '1 hour'. Use 'Inf' to read all messages. If enabled, features will not be available for query until
                                             all existing messages are processed. However, feature listeners will still be invoked as normal.
                                             See :ref:`kafka_initial_load`
``kafka.consumer.count``             Integer Number of kafka consumers used per feature type. Set to 0 to disable consuming (i.e. producer only)
``kafka.consumer.start-on-demand``   Boolean Start consuming a topic only when that feature type is first requested. This can reduce load if some
                                             layers are never queried
``kafka.topic.partitions``           Integer Number of partitions to use in new kafka topics
``kafka.topic.replication``          Integer Replication factor to use in new kafka topics
``kafka.serialization.type``         String  Internal serialization format to use for kafka messages. Must be one of ``kryo`` or ``avro``
``kafka.cache.expiry``               String  Expire features from in-memory cache after this delay, e.g. "10 minutes". See :ref:`kafka_expiry`
``kafka.cache.event-time``           String  Instead of message time, determine expiry based on feature data. See :ref:`kafka_event_time`
``kafka.cache.event-time.ordering``  Boolean Instead of message time, determine feature ordering based on the feature event time.
                                             See :ref:`kafka_event_time`
``kafka.index.cqengine``             String  Use CQEngine-based attribute indices for the in-memory feature cache. See :ref:`kafka_cqengine`
``kafka.index.resolution.x``         Integer Number of bins in the x-dimension of the spatial index, by default 360. See
                                             :ref:`kafka_index_resolution`
``kafka.index.resolution.y``         Integer Number of bins in the y-dimension of the spatial index, by default 180. See
                                             :ref:`kafka_index_resolution`
``kafka.index.tiers``                String  Number and size of tiers used for indexing geometries with extents, in the form ``x1:y1,x2:y2``.
                                             See :ref:`kafka_ssi`
``kafka.serialization.lazy``         Boolean Use lazy deserialization of features. This may improve processing load at
                                             the expense of slightly slower query times
``geomesa.query.loose-bounding-box`` Boolean Use loose bounding boxes, which offer improved performance but are not exact
``geomesa.query.audit``              Boolean Audit incoming queries. By default audits are written to a log file
``geomesa.security.auths``           String  Default authorizations used to query data, comma-separated
==================================== ======= ====================================================================================================

More information on using GeoTools can be found in the `GeoTools user guide
<http://docs.geotools.org/stable/userguide/>`__.
