.. _kafka_index:

Kafka Data Store
================

.. note::

    GeoMesa currently supports Kafka {{kafka_supported_versions}}.

The GeoMesa Kafka Data Store is an implementation of the GeoTools ``DataStore`` interface that is backed by `Apache Kafka`_.
The Kafka data store differs from most data stores in that queries are not run against the persisted data (which is stored in
Kafka topics). Instead, each data store will consume the data topic, keep it cached in local memory, and run queries against
the in-memory cache.

.. _Apache Kafka: https://kafka.apache.org/

To get started with the Kafka Data Store, try the :doc:`/tutorials/geomesa-quickstart-kafka` tutorial.

.. toctree::
   :maxdepth: 1

   install
   usage
   read_write
   geoserver
   commandline
   index_config
   data
   message_processing
   transactional_writes
   layer_views
   confluent
   streams
   zookeeper
