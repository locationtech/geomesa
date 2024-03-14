Data Consumers
==============

A GeoMesa Kafka data store will read features written to Kafka by
another GeoMesa Kafka data store (or itself). It will continually
pull data from the Kafka message queue and always represents the
latest state of the simple features.

.. note::

    Kafka data stores only meant for writing can disable consuming messages by setting
    the data store configuration ``kafka.consumer.count`` to 0.

First, create the data store. For example:

.. code-block:: java

   import org.geotools.api.data.DataStoreFinder;

    String brokers = ...
    String zookeepers = ...

    // build parameters map
    Map<String, Serializable> params = new HashMap<>();
    params.put("kafka.brokers", brokers);
    params.put("kafka.zookeepers", zookeepers);

    // optional - to read all existing messages on the kafka topic
    params.put("kafka.consumer.from-beginning", java.lang.Boolean.TRUE);

    // create the data store
    KafkaDataStore ds = (KafkaDataStore) DataStoreFinder.getDataStore(params);

``kakfa.brokers``, ``kafka.zookeepers`` (and optional ``kafka.zk.path``) must be
consistent with the values used to create the Kafka data store producer.

It is assumed that ``createSchema`` was already called by a Kafka data store producer.

.. code-block:: java

    String typeName = ...
    SimpleFeatureStore store = ds.getFeatureSource(typeName);

    Filter filter = ...
    store.getFeatures(filter);

Note that the data store will not start consuming features from Kafka for a given
``SimpleFeatureType`` until it is accessed, through either ``getFeatureSource()`` or ``getFeatureReader()``.
Once accessed, the store will continue consuming messages until ``dispose()`` is called.
