.. _lambda_overview:

Overview of the Lambda Data Store
=================================

The Lambda data store is intended for advanced use cases that involve frequent data updates combined with long-term
storage. For example, if you want to store ongoing GPS tracks, you could model each track as a single
``SimpleFeature`` with a line string geometry. As you receive GPS coordinates, you can modify the line string and
summary attributes, then update the ``SimpleFeature`` using
``DataStore.getFeatureWriter(filter, Transaction.AUTO_COMMIT)``. In a traditional ``DataStore``, this may be an
expensive operation, as it requires a synchronous query before the write. The Lambda data store uses Apache Kafka's
ordered log to allow for update writes without querying.

Basic Architecture
------------------

The Lambda data store consists of an in-memory cache of recent updates combined with a delegate data store for
long-term storage. In order to synchronize across instances, every write operation sends a message to a Kafka topic.
Each data store instance consumes the topic and loads the ``SimpleFeature`` into its in-memory cache. After
a configurable time-to-live without any updates for a given feature, the feature will be persisted to the delegate
data store and removed from the cache. The Lambda data store instances use Apache Zookeeper to synchronize cache
state, ensuring a feature is only written once. Queries against the store will merge results from the cache and
long-term storage.

Alternate Solutions
-------------------

If features are being added, but not subsequently updated, then an Accumulo, HBase or Cassandra data store can be
used directly, without the added complexity of Kafka. The Lambda data store could still be used as an in-memory
cache of recent features.

If features don't need to be stored long-term, then the Kafka data store can be used, without the complexity of
managing persistent features. The Lambda data store could still be used with ``persist`` disabled.

Integration with Other Data Stores
----------------------------------

A Lambda schema can be applied on top of an existing Accumulo schema. Additionally, features
persisted to Accumulo by the Lambda store can be accessed through an Accumulo data store. This allows for
existing Accumulo tools and analytics (GeoMesa Spark, etc) to be used on data from a Lambda store.
