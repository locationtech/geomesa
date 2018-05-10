Kafka Index Configuration
=========================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
Because a Kafka data store maintains an in-memory cache of features, most of these options can be configured on
a consumer data store and take immediate effect.

.. _kafka_initial_load:

Initial Load (Replay)
---------------------

By default, a Kafka consumer data store will start consuming from the end of a topic. This means that it will
only see new updates that are written after it has spun up. Optionally, the consumer may start from the beginning
of the topic, by setting ``kafka.consumer.from-beginning`` to ``true`` in the data store parameters. This
allows a consumer to replay old messages and establish a baseline state. Note that a feature store will not return
any query results during this initial load, until it has caught up to head state.

Also see :ref:`topic_compaction` for details on managing the size and history of the Kafka topic.

Feature Expiration
------------------

Generally, a Kafka consumer data store will keep any features that are written by a producer data store,
until they are explicitly deleted by the producer using a modifying feature writer. Optionally, a consumer
data store may expire features after a certain timeout, by specifying the ``kafka.cache.expiry`` data store
parameter. When a producer writes an update to an existing feature, the consumer will reset the expiration timeout.
Once the timeout is hit without any updates, the feature will be removed from the consumer cache and will no
longer be returned when querying.

Spatial Index Resolution
------------------------

The Kafka consumer data store uses an in-memory spatial index for querying. The spatial index breaks the world up
into a coarse grid, and then only examines the relevant grid cells when running a spatial query. The grid size
can be modified by setting the ``kafka.index.resolution.x`` and/or ``kafka.index.resolution.y`` data
store parameters. By default, the grid is 360 by 180 cells. Increasing the grid resolution may reduce the
number of false-positive features that must be considered when querying, and can reduce contention between
simultaneous updates, deletes and queries. However, it also requires more memory.

CQEngine Indexing
-----------------

By default, a Kafka consumer data store only creates a spatial index. Any other queries (for example, date queries)
will have to iterate over all the features in the index. Generally the number of features is reasonable, and this
is still a fast operation.

For more advanced use-cases, additional in-memory index structures can be created to satisfy non-spatial queries.
This can be enabled by setting the ``kafka.cache.cqengine`` data store parameter to ``true``. This will enable
`CQEngine <https://github.com/npgall/cqengine>`__ indices on each SimpleFeature attribute. Note that this
may require more processing than the standard index. Also, as each attribute will be read on load, it should
not be used with lazy deserialization (see next).


Lazy Deserialization
--------------------

By default, a Kafka consumer data store will use lazy (on-demand) deserialization of feature attributes. For
rendering maps (which usually only require the geometry attribute) or for write-heavy workflows, this can avoid
the overhead of deserializing and instantiating attributes that may never be read. If writes are infrequent, or
all features and attributes are consistently read, then lazy deserialization can be disabled by setting the
``kafka.serialization.lazy`` data store parameter to ``false``. Lazy deserialization incurs a very small
runtime (query) penalty, as each attribute has to be checked for deserialization before being returned.
