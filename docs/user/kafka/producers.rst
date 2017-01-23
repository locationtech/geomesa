Data Producers
==============

A GeoMesa Kafka data store in *producer* mode takes features and persists them to Kafka
topics. First, create the data store. For example:

.. code-block:: java

    String brokers = ...
    String zookeepers = ...
    String zkPath = ...

    // build parameters map
    Map<String, Serializable> params = new HashMap<>();
    params.put("brokers", brokers);
    params.put("zookeepers", zookeepers);
    params.put("isProducer", Boolean.TRUE);

    // optional
    params.put("zkPath", zkPath);

    // create the data store
    KafkaDataStoreFactory factory = new KafkaDataStoreFactory();
    DataStore producerDs = factory.createDataStore(params);

Next, create the schema. Each data store can have one or many schemas.
For example:

.. code-block:: java

    SimpleFeatureType sft = ...
    SimpleFeatureType streamingSFT = KafkaDataStoreHelper.createStreamingSFT(sft, zkPath);
    producerDs.createSchema(streamingSFT);

The call to ``KafkaDataStoreHelper.createSchema`` creates a copy of the
``sft`` with the required hint added. In this case the hint is the name
of the Kafka topic. The ``zkPath`` parameter is uses to make the Kafka
topic name unique to the ``zkPath`` used by the ``KafkaDataStore`` so
that the same ``SimpleFeatureType`` can be used by multiple
``KafkaDataStore``\ s where each data store has a different ``zkPath``.
Specifically, the resulting Kafka topic's name will be zkPath-sftName
where the forward slashes in the ``zkPath`` are replaced by ``-``.  e.g. creating a schema
with a ``zkPath`` of /geomesa/ds/kafka with an ``sft`` called example_sft will
create a Kafka topic called geomesa-ds-kafka-example_sft.

The ``createSchema`` method will throw an exception if the given
``SimpleFeatureType`` does not contain the required hint, i.e., if it
was not created by the ``KafkaDataStoreHelper``.

Now, you can create or update simple features:

.. code-block:: java

    // the name of the simple feature type -  will be the same as sft.getTypeName();
    String typeName = streamingSFT.getTypeName();

    FeatureWriter<SimpleFeatureType, SimpleFeature> fw =
            producerDs.getFeatureWriter(typeName, null, Transaction.AUTO_COMMIT);
    SimpleFeature sf = fw.next();
    // set properties on sf
    fw.write();

Delete simple features:

.. code-block:: java

    SimpleFeatureStore producerStore = (SimpleFeatureStore) producerDs.getFeatureSource(typeName);
    FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

    String id = ...
    producerStore.removeFeatures(ff.id(ff.featureId(id)));

And, clear (delete all) features:

.. code-block:: java

    producerStore.removeFeatures(Filter.INCLUDE);

Each operation that creates, modifies, deletes, or clears simple
features results in a message being sent to the Kafka topic.
