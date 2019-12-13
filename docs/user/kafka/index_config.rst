.. _kafka_index_config:

Kafka Index Configuration
=========================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
Because a Kafka data store maintains an in-memory cache of features, most of these options can be configured on
a consumer data store and take immediate effect.

.. _kafka_initial_load:

Initial Load (Replay)
---------------------

By default, a Kafka consumer data store will start consuming from the end of a topic. This means that it will
only see new updates that are written after it has spun up. Optionally, the consumer may start from earlier
in the topic, by setting ``kafka.consumer.read-back`` to a duration, such as ``1 hour``, in the data store
parameters. This allows a consumer to replay old messages and establish a baseline state. To read the entire
message queue, use the value ``Inf``.

Reading back by a given interval is only supported in Kafka starting with version 0.10.1. Older versions will fall
back to reading from the very beginning of the topic.

Note that a feature store will not return any query results during this initial load, until it has caught up to
head state.

Also see :ref:`topic_compaction` for details on managing the size and history of the Kafka topic.

.. _kafka_expiry:

Feature Expiration
------------------

Generally, a Kafka consumer data store will keep any features that are written by a producer data store,
until they are explicitly deleted by the producer using a modifying feature writer. Optionally, a consumer
data store may expire features after a certain timeout, by specifying the ``kafka.cache.expiry`` data store
parameter. When a producer writes an update to an existing feature, the consumer will reset the expiration timeout.
Once the timeout is hit without any updates, the feature will be removed from the consumer cache and will no
longer be returned when querying.

.. _kafka_event_time:

Feature Event Time
------------------

By default, expiration and updates are determined by Kafka message time. Feature updates will replace any
prior feature message, and feature will expire based on when they were read. Alternatively, one or both
of these values may be based on feature attributes.

To enable event time, specify a property name or CQL expression using the ``kafka.cache.event-time`` data store
parameter. This expression will be evaluated on a per-feature basis, and must evaluate to either a date or a
number representing milliseconds since the Java epoch. This value will be combined with the ``kafka.cache.expiry``
value to set an expiration time for the feature.

To also enable event time ordering, set the ``kafka.cache.event-time.ordering`` data store parameter to ``true``.
When enabled, if a feature update is read that has an older event time than the current feature, the message
will be discarded. This can be useful for handling irregular update streams.

.. _kafka_index_resolution:

Spatial Index Resolution
------------------------

The Kafka consumer data store uses an in-memory spatial index for querying. The spatial index breaks the world up
into a coarse grid, and then only examines the relevant grid cells when running a spatial query. The grid size
can be modified by setting the ``kafka.index.resolution.x`` and/or ``kafka.index.resolution.y`` data
store parameters. By default, the grid is 360 by 180 cells. Increasing the grid resolution may reduce the
number of false-positive features that must be considered when querying, and can reduce contention between
simultaneous updates, deletes and queries. However, it also requires more memory.

.. _kafka_ssi:

Spatial Index Tiering
---------------------

For geometries with extents (i.e. non-points), the Kafka consumer data store uses a tiered in-memory spatial index
for querying. Geometries are stored in a tier based on their envelope size. The number and size (in degrees) of
tiers can be modified by setting the ``kafka.index.tiers`` data store parameter. By default, four tiers are created
of sizes ``1x1``, ``4x4``, ``32x32`` and ``360x180``. In general, you want the tiers to correspond to the size
of the geometries you are indexing. Geometries which are larger than any of the available tiers will not be
indexable; thus it is standard to include a 'catch-all' tier that encompases the whole world.

Tiers may be specified by comma-separated pairs of numbers, where each pair is separated with a ``:``. For example,
the default tiers would be specified as ``1:1,4:4,32:32,360:180``.

.. _kafka_cqengine:

CQEngine Indexing
-----------------

By default, a Kafka consumer data store only creates a spatial index. Any other queries (for example, date queries)
will have to iterate over all the features in the index. Generally the number of features is reasonable, and this
is still a fast operation.

For more advanced use-cases, additional in-memory index structures can be created to satisfy non-spatial queries.
This can be enabled by setting the ``kafka.index.cqengine`` data store parameter. The value should
be a comma-delimited list of ``name:type``, where `name` is an attribute name and `type` is a CQEngine index
type. See :ref:`in_memory_index` for more information. Note that when using CQEngine, the default geometry will
not be indexed without an explicit configuration (e.g. ``geom:geometry`` in the parameter value). In addition,
CQEngine may require more processing than the standard index.

As an example, consider the schema ``name:String,age:Int,dtg:Date,*geom:Point:srid=4326``. To create an index
on each attribute, you could set ``kafka.index.cqengine`` to
``name:radix,age:default,dtg:navigable,geom:geometry``. See :ref:`in_memory_index` for an explanation of the
index types.

Lazy Deserialization
--------------------

By default, a Kafka consumer data store will use lazy (on-demand) deserialization of feature attributes. For
rendering maps (which usually only require the geometry attribute) or for write-heavy workflows, this can avoid
the overhead of deserializing and instantiating attributes that may never be read. If writes are infrequent, or
all features and attributes are consistently read, then lazy deserialization can be disabled by setting the
``kafka.serialization.lazy`` data store parameter to ``false``. Lazy deserialization incurs a very small
runtime (query) penalty, as each attribute has to be checked for deserialization before being returned.
