Using the Kafka Data Store Programmatically
===========================================

To create a ``KafkaDataStore`` there are two required properties, one
for the Apache Kafka connection, "brokers", and one for the Apache
Zookeeper connection, "zookeepers". An optional parameter, "zkPath" is
used to specify a path in Zookeeper under which schemas are stored. If
no "zkPath" is specified then a default path will be used. Another
optional parameter, "isProducer", is used to create a ``KafkaDataStore``
in *producer* or *consumer* mode. This parameter defaults to false, i.e.
by default a Kafka Consumer Data Store will be created. The same set of
configuration parameters, with the exception of "isProducer" must be
used to create both the Kafka Producer Data Store and the Kafka Consumer
Data Store.

After a ``KafkaDataStore`` has been created, additional simple feature
type specific hints must be provided. These hints are stored in the user
data of the ``SimpleFeatureType``. Use the ``KafkaDataStoreHelper`` to
create a copy of your ``SimpleFeatureType`` with the hints added. Then
call ``dataStore.createSchema(sft)`` where ``sft`` is the
``SimpleFeatureType`` returned by the ``KafkaDataStoreHelper``. This
must be done once per Simple Feature Type. The Simple Feature Type along
with hints are stored in Zookeeper so if ``createSchema(sft)`` is called
on the Kafka Data Store Producer, it cannot be called on the Kafka
Consumer Data Store Consumer.

.. _kafka_parameters:

Parameters
----------

The Kafka Data Store takes several parameters.

================= ======== ====================================================================
Parameter         Type     Description
================= ======== ====================================================================
brokers *         String   Comma-separated list of Kafka brokers
zookeepers *      String   Comma-separated list of Zookeeper servers
zkPath            String   Path to GeoMesa Kafka data in Zookeeper
namespace         String   Namespace
partitions        Integer  Number of partitions to use in Kafka topics
replication       Integer  Replication factor to use in Kafka topics
isProducer        Boolean  Indicates whether the Kafka Data Store is a producer
expirationPeriod  Long     Feature will be "auto-dropped" (expired) after this delay
                           in milliseconds. Leave black or use -1 to not drop features.
cleanUpCache      Boolean  If true, run a thread to clean up the live feature cache every
                           second if set to true. False by default.
autoOffsetReset   String   What offset to reset to when there is no initial offset in ZooKeeper 
                           or if an offset is out of range. Valid values are smallest or 
                           largest. Default value is largest.
================= ======== ====================================================================

The required parameters are marked with an asterisk.