Data Management
===============

Kafka Topic Name
----------------

Each SimpleFeatureType (or schema) will be written to a unique Kafka topic. By default, the topic name will consist of the value
of the ``kafka.catalog.topic`` data store parameter (or ``kafka.zk.path`` if using Zookeeper), and the SimpleFeatureType name,
separated with a ``-``. For example, with the default catalog topic (``geomesa-catalog``), a SimpleFeatureType name of ``my-type``
would result in the topic ``geomesa-catalog-my-type``.

If desired, the topic name can be set to an arbitrary value by setting the user data key ``geomesa.kafka.topic``
before calling ``createSchema``:

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("geomesa.kafka.topic", "myTopicName");

For more information on how to set schema options, see :ref:`set_sft_options`.

Kafka Topic Configuration
-------------------------

The Kafka topic for a given SimpleFeatureType will be created when calling ``createSchema`` (if it doesn't already
exist). GeoMesa exposes a few configuration options through data store parameters. Additional options can
be configured by setting the user data key ``kafka.topic.config`` before calling ``createSchema``:

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("kafka.topic.config", "cleanup.policy=compact\nretention.ms=86400000");

The value should be in standard Java properties format. For a list of available configurations, refer
to the `Kafka documentation <https://kafka.apache.org/documentation/#topicconfigs>`__. For more information
on how to set schema options, see :ref:`set_sft_options`.

Parallelism in Kafka is achieved through the use of multiple topic partitions. Each partition can only be read
by a single Kafka consumer. The number of consumers can be controlled through the ``kafka.consumer.count`` data
store parameter; however, this will have no effect if there is only a single topic partition. To create more than
one partition, use the ``kafka.topic.partitions`` data store parameter.

Replication in Kafka ensures that data is not lost. To enable replication, use the ``kafka.topic.replication``
data store parameter.

.. _topic_compaction:

Kafka Topic Compaction
----------------------

Kafka has various options for preventing data from growing unbounded. The simplest is to set a size or time-based
retention policy. This will cause older messages to be deleted when the topic reaches a certain threshold.

Starting with GeoMesa 2.1.0, the Kafka data store supports Kafka
`log compaction <https://kafka.apache.org/10/documentation.html#compaction>`__. This allows for the topic size
to be managed, while preserving the latest state for each feature. When combined with :ref:`kafka_initial_load`,
the persistent state of a system can be maintained through restarts and down-time. Note that when using log
compaction, it is important to send explicit deletes for each feature, otherwise the feature will never be
compacted out from the log, and the log size will start to grow unbounded.

If upgrading from a version of GeoMesa prior to 2.1.0, the topic should be run for a while using a size or
time-based retention policy before enabling compaction, as messages written with older versions of GeoMesa will
never be compacted out.

.. _kafka_serialization_format:

Serialization Format
--------------------

The Kafka message serialization format may be configured with the data store parameter ``kafka.serialization.type``.

By default, Kafka messages are serialized with a custom Kryo serializer. The Kryo serializer has the ability to only deserialize
the fields of a feature that are being read, which can greatly reduce processing load when dealing with high-velocity data
streams (where many features may not be queried before they are updated) and/or WMS (map) queries (which often only read a small
number of attributes).

For broader compatibility, Avro serialization can be used instead. Avro libraries exist in many languages, and Avro messages
follow a defined schema that allows for cross-platform parsing. When using Avro, the message schema can be obtained through
the :ref:`gen_avro_schema_cli` command line method.

GeoMesa supports two flavors of Avro serialization, ``avro-native`` (preferred) or ``avro`` (legacy). The ``avro`` format
is similar to ``avro-native``, but collection-type attributes (Maps and Lists) are encoded in an opaque fashion, instead
of using native Avro collection types. It should only be used if there is a need to work with older GeoMesa versions that
don't support ``avro-native``.

Integration with Other Systems
------------------------------

The Kafka data store is easy to integrate with by consuming the Kafka topic. The messages are a change log of
updates. Message keys consist of the simple feature ID, as UTF-8 bytes. Message bodies are serialized simple
features, or null to indicate deletion. The internal serialization version is set as a message header under the
key ``"v"``. See :ref:`kafka_serialization_format` for details on message serialization.

If you are using the Confluent platform to manage Kafka, please see :ref:`confluent_kds`.
