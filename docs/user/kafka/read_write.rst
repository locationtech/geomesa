Producers and Consumers
=======================

The Kafka data store differs from most data stores in that queries are not run against the persisted data (which is stored in
Kafka topics). Instead, each data store will consume the data topic, keep it cached in local memory, and run queries against
the in-memory cache. As such, it is often useful to separate data store instances by whether they are **producers** (writing
data) or **consumers** (reading data and being queried).

Data Producers
--------------

A GeoMesa Kafka data store can act as a Kafka producer and write features to a Kafka topic.

First, create the data store:

.. code-block:: java

    import org.geotools.api.data.DataStoreFinder;

    // build parameters map
    Map<String, String> params =
      Map.of(
        "kafka.brokers", "localhost:9092",
        "kafka.catalog.topic", "geomesa-catalog", // optional - specify topic used to persist schema metadata
        "kafka.consumer.count", "0" // optional - disable consuming messages for this data store
      );


    // create the data store
    KafkaDataStore ds = (KafkaDataStore) DataStoreFinder.getDataStore(params);

.. note::

    Kafka data stores only meant for writing can disable consuming messages by setting
    the data store configuration ``kafka.consumer.count`` to 0.

Next, create the schema. Each data store can have multiple schemas. For example:

.. code-block:: java

    SimpleFeatureType sft = ...
    ds.createSchema(sft);

Now, you can create or update simple features. Note that the Kafka data store only supports update
via feature ID. You may explicitly create a modifying feature writer with an ID filter, or simply use
an append feature writer, which will overwrite any existing feature with the same feature ID:

.. code-block:: java

    // the name of the simple feature type -  will be the same as sft.getTypeName();
    String typeName = sft.getTypeName();

    // use try-with-resources to clean up the writer
    try (SimpleFeatureWriter fw = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) {
      SimpleFeature sf = fw.next();
      // set properties on sf
      sf.setAttribute("name", "Alice");
      // write the feature to Kafka
      fw.write();
    }

Note that messages may be cached/buffered before they are sent out by the Kafka producer. The producer behavior
can be configured via the data store parameter ``kafka.producer.config`` (see :ref:`kafka_parameters` for details).
Messages will be flushed when the feature writer is closed. Additionally, the writer also implements ``java.io.Flushable``
for fine-grained control.

.. note::

    When using a modifying feature writer, the features returned will not have the attributes
    of the actual current features, but will have the correct feature ID.

.. warning::

    To get the Kafka feature writer to use the provided feature ID, the standard GeoTools
    ``Hints.USE_PROVIDED_FID`` or ``Hints.PROVIDED_FID`` must be used. Otherwise, a new
    feature ID will be generated.

To delete features:

.. code-block:: java

    SimpleFeatureStore store = (SimpleFeatureStore) ds.getFeatureSource(typeName);
    FilterFactory ff = CommonFactoryFinder.getFilterFactory();

    String id = ...
    store.removeFeatures(ff.id(ff.featureId(id)));

And, to clear (delete all) features:

.. code-block:: java

    store.removeFeatures(Filter.INCLUDE);

Each operation that creates, modifies, deletes, or clears simple
features results in a message being sent to a Kafka topic.

Data Consumers
--------------

A GeoMesa Kafka data store will read features written to Kafka by
another GeoMesa Kafka data store (or itself). It will continually
pull data from the Kafka message queue and always represents the
latest state of the simple features.

First, create the data store. For example:

.. code-block:: java

   import org.geotools.api.data.DataStoreFinder;

    // build parameters map
    Map<String, Serializable> params =
      Map.of(
        "kafka.brokers", "localhost:9092",
        "kafka.catalog.topic", "geomesa-catalog", // optional - specify topic used to persist schema metadata
        "kafka.consumer.read-back", "1 hour" // optional - to read existing messages on the kafka topic
      );

    // create the data store
    KafkaDataStore ds = (KafkaDataStore) DataStoreFinder.getDataStore(params);

The ``kafka.brokers`` and ``kafka.catalog.topic`` must be consistent with the values used to write data.

.. code-block:: java

    String typeName = ...
    SimpleFeatureStore store = ds.getFeatureSource(typeName);

    Filter filter = ...
    store.getFeatures(filter);

Note that by default the data store will not start consuming features from Kafka for a given ``SimpleFeatureType`` until it is
accessed, through either ``getFeatureSource()`` or ``getFeatureReader()`` (this may be overridden with the
``kafka.consumer.start-on-demand`` data store parameter). Once accessed, the store will continue consuming messages until
``dispose()`` is called.
