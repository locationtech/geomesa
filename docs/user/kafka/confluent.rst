.. _confluent_kds:

Confluent Integration
=====================

.. warning::

  Confluent integration is currently experimental and supports consuming from Kafka, but not producing.

The Kafka data store can integrate with Confluent Kafka topics and the Confluent Schema Registry. The schema
registry is a centralized store of versioned Avro schemas, each associated with a particular Kafka topic.

To read from a Confluent topic, set the URL to the schema registry in your data store parameter map under the key
``kafka.schema.registry.url``. In GeoServer, select the "Confluent Kafka (GeoMesa)" store instead of the
regular Kafka store.

Note that Confluent requires Avro 1.8 and the Confluent client JARs, which are not bundled with GeoMesa.
