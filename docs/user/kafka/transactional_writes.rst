.. _kafka_transactional_writes:

Transactional Writes
--------------------

Kafka supports the concept of `transactional writes`_. GeoMesa exposes this functionality
through the GeoTools `transaction API`_:

.. code-block:: java

    import org.geotools.api.data.DataStore;
    import org.geotools.api.data.DataStoreFinder;
    import org.geotools.api.data.FeatureWriter;
    import org.geotools.api.data.Transaction;
    import org.geotools.data.DefaultTransaction;

    DataStore store = DataStoreFinder.getDataStore(params);
    // the transaction will contain the Kafka producer, so make sure to close it when finished
    try (Transaction transaction = new DefaultTransaction()) {
        // pass the transaction when getting a feature writer
        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                    store.getFeatureWriterAppend("my-type", transaction)) {
             // write some features (elided), then commit the transaction:
            transaction.commit();
            // if you get an error (elided), then rollback the transaction:
            transaction.rollback();
        }
        // re-using the transaction will re-use the Kafka producer
        try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                   store.getFeatureWriterAppend("my-type", transaction)) {
            // write some features (elided), then commit the transaction:
            transaction.commit();
        }
    }

.. _transactional writes: https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
.. _transaction API: https://docs.geotools.org/stable/javadocs/org/geotools/api/data/Transaction.html
