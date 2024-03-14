Data Producers
==============

A GeoMesa Kafka data store can act as a Kafka producer and write features to a Kafka topic.

.. note::

    Kafka data stores only meant for writing can disable consuming messages by setting
    the data store configuration ``kafka.consumer.count`` to 0.

First, create the data store:

.. code-block:: java

    import org.geotools.api.data.DataStoreFinder;

    String brokers = ...
    String zookeepers = ...

    // build parameters map
    Map<String, Serializable> params = new HashMap<>();
    params.put("kafka.brokers", brokers);
    params.put("kafka.zookeepers", zookeepers);

    // create the data store
    KafkaDataStore ds = (KafkaDataStore) DataStoreFinder.getDataStore(params);

Next, create the schema. Each data store can have 0 to many schemas. For example:

.. code-block:: java

    SimpleFeatureType sft = ...
    ds.createSchema(sft);

Now, you can create or update simple features. Note that the Kafka data store only supports update
via feature ID. You may explicitly create a modifying feature writer with an ID filter, or simply use
an append feature writer, which will overwrite any existing feature with the same feature ID:

.. code-block:: java

    // the name of the simple feature type -  will be the same as sft.getTypeName();
    String typeName = sft.getTypeName();

    SimpleFeatureWriter fw = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT);
    SimpleFeature sf = fw.next();
    // set properties on sf
    fw.write();

.. note::

    When using a modifying feature writer, the features returned will not have the attributes
    of the actual current features, but will have the correct feature ID.

.. warning::

    To get the Kafka feature writer to use the provided feature ID, the standard GeoTools
    ``Hints.USE_PROVIDED_FID`` or ``Hints.PROVIDED_FID`` must be used. Otherwise, a new
    feature ID will be generated.

Delete simple features:

.. code-block:: java

    SimpleFeatureStore store = (SimpleFeatureStore) ds.getFeatureSource(typeName);
    FilterFactory ff = CommonFactoryFinder.getFilterFactory();

    String id = ...
    store.removeFeatures(ff.id(ff.featureId(id)));

And, clear (delete all) features:

.. code-block:: java

    store.removeFeatures(Filter.INCLUDE);

Each operation that creates, modifies, deletes, or clears simple
features results in a message being sent to a Kafka topic.
