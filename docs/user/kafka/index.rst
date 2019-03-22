.. _kafka_index:

Kafka Data Store
================

.. note::

    GeoMesa currently supports Kafka version |kafka_version|. However, not all features are supported
    for versions prior to 1.0.

The GeoMesa Kafka Data Store is an implementation of the GeoTools
``DataStore`` interface that is backed by `Apache Kafka`_. The
implementation supports the ability for feature producers to instantiate
a Kafka Data Store in *producer* mode to persist data into the data
store and for consumers to instantiate a Kafka Data Store in
*consumer* mode to read data from the data store. The producer and
consumer data stores can be run on separate servers. The only
requirement is that they can connect to the same instance of Apache
Kafka.

.. _Apache Kafka: http://kafka.apache.org/

All of the Kafka-specific code for GeoMesa is found in the ``geomesa-kafka``
directory of the source distribution.

To get started with the Kafka Data Store, try the :doc:`/tutorials/geomesa-quickstart-kafka` tutorial.

.. toctree::
   :maxdepth: 1

   install
   usage
   geoserver
   commandline
   producers
   consumers
   index_config
   data
   feature_events
   confluent
