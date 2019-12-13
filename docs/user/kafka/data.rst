Data Management
===============

Kafka Topic Name
----------------

Each SimpleFeatureType (or schema) will be written to a unique Kafka topic. By default, the topic will be
named based on the ``kafka.zk.path`` data store parameter and the SimpleFeatureType name, by appending
the two together and replacing any ``/`` characters with ``-``. For example, with the default zookeeper path
(``geomesa/ds/kafka``), a SimpleFeatureType name of 'foo' would result in the topic ``geomesa-ds-kafka-foo``.

If desired, the topic name can be set to an arbitrary value by setting the user data key ``geomesa.kafka.topic``
before calling ``createSchema``:

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("geomesa.kafka.topic", "myTopicName");

For more information on how to set schema options, see :ref:`set_sft_options`.

Kafka Topic Configuration
-------------------------

The Kafka topic for a given SimpleFeatureType will be created when calling ``createSchema`` (if it doesn't already
exist). GeoMesa exposes a few configuration options through data store parameters. For more advanced options,
the topic should be created using standard Kafka tools, and then the existing topic should be specified by name
for the SimpleFeatureType (as described above).

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

Integration with Other Systems
------------------------------

The Kafka data store is easy to integrate with by consuming the Kafka topic. The messages are a change log of
updates. Message keys consist of the simple feature ID, as UTF-8 bytes. Message bodies are serialized simple
features, or null to indicate deletion. The internal serialization version is set as a message header under the
key ``"v"``, when using Kafka 0.11.x or newer.

By default, message bodies are serialized with a custom Kryo serializer. For Java/Scala clients, the
``org.locationtech.geomesa.features.kryo.KryoFeatureSerializer`` class may be used to decode messages, available
in the ``geomesa-feature-kryo_2.11`` module through Maven. Alternatively, producers can be configured to send
Avro-encoded messages through the ``kafka.serialization.type`` data store parameter. Avro libraries exist in many
languages, and Avro messages follow a defined schema that allows for cross-platform parsing.

If you are using the Confluent platform to manage Kafka, please see :ref:`confluent_kds`.
