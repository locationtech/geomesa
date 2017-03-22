Data Consumers
==============

A GeoMesa Kafka data store in *consumer* mode reads feature data written
to Kafka (by a separate GeoMesa Kafka producer). The store supports two types of
``SimpleFeatureSource``'s: *live* and *replay*. A Kafka Consumer Feature
Source operating in *live* mode continually pulls data from the end of
the message queue (e.g. latest time) and always represents the latest
state of the simple features; ``autoOffsetReset`` can be set to ``smallest``
to read from the beginning of the the message queue. A Kafka Consumer Feature
Source operating in *replay* mode will pull data from a specified time interval
in the past and can provide features as they existed at any point in time
within that interval.

First, create the data store. For example:

.. code-block:: java

    String brokers = ...
    String zookeepers = ...
    String zkPath = ...

    // build parameters map
    Map<String, Serializable> params = new HashMap<>();
    params.put("brokers", brokers);
    params.put("zookeepers", zookeepers);

    // optional - the default is false
    params.put("isProducer", Boolean.FALSE);

    // optional
    params.put("zkPath", zkPath);

    // optional - can be set to "smallest" to read from the beginning of the message queue
    params.put("autoOffsetReset", "largest");

    // create the data store
    KafkaDataStoreFactory factory = new KafkaDataStoreFactory();
    DataStore consumerDs = factory.createDataStore(params);

The ``brokers``, ``zookeepers``, and ``zkPath`` parameters must be
consistent with the values used to create the Kafka Data Store Producer.

Because ``createSchema`` was called on the Kafka Data Store Producer, it
does not need to be called on the Consumer. Calling ``createSchema``
with a ``SimpleFeatureType`` that has already been created will result
in an exception being thrown. Note that all ``SimpleFeature``\ s
returned by the Kafka Data Store consumer will have a
``SimpleFeatureType`` equal to the ``streamingSFT`` created when setting
up the producer, i.e. the ``SimpleFeatureType`` will include the hint
added by ``KafkaDataStoreHelper.createStreamingSFT``.

Now that the Kafka Data Store Consumer has been created it can be
queried in either *live* or *replay* mode.

Live Mode
---------

Live mode is the default and requires no extra setup. In this mode the
``SimpleFeatureSource`` contains the current state of the
``KafkaDataStore``. As ``SimpleFeatures`` are created, modified,
deleted, or cleared by the Kafka Data Store Producer, the current state
is updated. All queries to the ``SimpleFeatureSource`` are queries
against the current state. For example:

.. code-block:: java

    String typeName = ...
    SimpleFeatureSource liveFeatureSource = consumerDs.getFeatureSource(typeName);

    Filter filter = ...
    liveFeatureSource.getFeatures(filter);

It is also possible to provide a CQL filter to the getFeatureSource method call which will ensure
the resulting ``FeatureSource`` only contains certain records. Providing a filter to reduce the number of
returned records will provide a performance boost when using the featureSource.

.. code-block:: java

    String typeName = ...
    SimpleFeatureSource liveFeatureSource = consumerDs.getFeatureSource(typeName, filter);

Replay Mode
-----------

Replay mode allows the a user to query the ``KafkaDataStore`` as it
existed at any point in the past. Queries against a Kafka Replay Simple
Feature source specify a historical time to query and only the set and
version of ``SimpleFeature``\ s that existed at that point in time will
be used to answer the query.

In order to use Replay mode some additional hints are required: the
start and end times of the replay window and a read behind duration:

.. code-block:: java

    Instant replayStart = ...
    Instant replayEnd = ...
    Duration readBehind = ...
    ReplayConfig replayConfig = new ReplayConfig(replayStart, replayEnd, readBehind);

The replay window is simply an optimization that allows the Kafka Replay
Feature Source to load, at initialization time, all state changes that
occur within the window. Any query for a time outside of the window will
return no results even if features existed at that time.

The read behind is the amount of time used to rebuild state. For
example, if ``readBehind = 5s`` then for a query requesting state at
``time = t`` all state changes that occurred between ``t - 5s`` and
``t`` will be used to build the state at time ``t`` which will then be
used to answer the query. Selecting an appropriate read behind requires
an understanding of the producer. The expected use case is a producer
that updates every simple feature, even if it hasn't changed, at a
regular interval. For example, if the producer is updating every ``x``
seconds then a read behind of ``x + 1s`` might be appropriate.

During initialization of the Kafka Replay Feature Source all state
changes from ``replayStart - readBehind`` to ``replayEnd`` will be read
and cached. As the size of the replay window and read behind increases
so does the amount of data that must be read and cashed. So, both the
size of the window and the read behind should be kept as small as
possible.

After creating the ``ReplayConfig``, pass it, along with the
``streamingSFT``, to the ``KafkaDataStoreHelper``:

.. code-block:: java

    SimpleFeatureType streamingSFT = consumerDs.getSchema(typeName);
    SimpleFeatureType replaySFT = KafkaDataStoreHelper.createReplaySFT(streamingSFT, replayConfig);

The ``streamingSFT`` passed to ``createReplaySFT`` must contain the
hints added by ``KafkaDataStoreHelper.createStreamingSFT``. The easiest
way to ensure this is to call ``consumerDs.getSchema(typeName)``. The
``SimpleFeatureType`` returned by ``createReplaySFT`` will contain the
hint added by ``createStreamingSFT`` as well as a a hint containing the
``ReplayConfig``. Additionally the ``replaySFT`` will have a different
name than ``streamingSFT``. This is to differentiate *live* and
*replay* ``SimpleFeatureType``\ s. The ``replaySFT`` will also contain
an additional attribute, ``KafkaLogTime``, of type ``java.util.Date``
which represents the historical query time.

After creating the ``replaySFT`` the Kafka Replay Feature Source may be
created:

.. code-block:: java

    consumerDs.createSchema(replaySFT);

    String replayTimeName = replaySFT.getTypeName();
    SimpleFeatureSource replayFeatureSource = consumerDs.getFeatureSource(replayTimeName);

The call to ``createSchema`` is required because the ``replaySFT`` is a
new ``SimpleFeatureType``.

Finally the Kafka Replay Consumer Feature Source can be queried:

.. code-block:: java

    Instant historicalTime = ...
    Filter timeFilter = ff.and(filter, ReplayTimeHelper.toFilter(historicalTime));

    replayFeatureSource.getFeatures(timeFilter);