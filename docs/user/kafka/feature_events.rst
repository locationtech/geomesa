.. _kafka_feature_events:

Kafka Feature Events
--------------------

Listening for Feature Events
============================

.. warning::

    The feature events API does not guarantee all messages will be fully processed. To ensure
    at-least-once processing, use :ref:`kafka_guaranteed_processing`.

The GeoTools API includes a mechanism to fire off a `FeatureEvent`_ object each time
that there is an "event," which occurs when data is added, changed, or deleted in a
`SimpleFeatureSource`_. A client may implement a `FeatureListener`_, which has a single
method called ``changed()`` that is invoked each time that each `FeatureEvent`_ is
fired.

Three types of messages are produced by a GeoMesa Kafka producer. Each message will
cause a `FeatureEvent`_ to be fired when read by a GeoMesa Kafka consumer. All feature
event classes extend ``org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent`` and are
contained in the companion object of the same name.

+----------------+-------------------------+----------------------+--------------------+
| Message read   | Class of event fired    | `FeatureEvent.Type`_ | Filter             |
+================+=========================+======================+====================+
| CreateOrUpdate | ``KafkaFeatureChanged`` | ``CHANGED``          | ``IN (<id>)``      |
+----------------+-------------------------+----------------------+--------------------+
| A single feature with a given id has been added; this may be a new feature or an     |
| update of an existing feature                                                        |
+----------------+-------------------------+----------------------+--------------------+
| Delete         | ``KafkaFeatureRemoved`` | ``REMOVED``          | ``IN (<id>)``      |
+----------------+-------------------------+----------------------+--------------------+
| The feature with the given id has been removed                                       |
+----------------+-------------------------+----------------------+--------------------+
| Clear          | ``KafkaFeatureCleared`` | ``REMOVED``          | ``Filter.INCLUDE`` |
+----------------+-------------------------+----------------------+--------------------+
| All features have been removed                                                       |
+----------------+-------------------------+----------------------+--------------------+

In addition to the normal information in a `FeatureEvent`_, CreateOrUpdate messages expose the
relevant ``SimpleFeature`` with the method ``feature()``. Delete messages expose the feature ID
with the method ``id()``, and also include the ``SimpleFeature`` if it is available (it may be null).
All events expose the original Kafka timestamp with the method ``time()``.

To register a `FeatureListener`_, create the `SimpleFeatureSource`_ from a GeoMesa
Kafka consumer data store, and use the ``addFeatureListener()`` method. For example, the
following listener simply prints out the events it receives:

.. code-block:: java

    import org.geotools.api.data.FeatureEvent;
    import org.geotools.api.data.FeatureListener;
    import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.KafkaFeatureChanged;
    import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.KafkaFeatureRemoved;
    import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.KafkaFeatureCleared;

    // unless specified, the consumer will only read data written after its instantiation
    SimpleFeatureSource source = ds.getFeatureSource(sftName);
    FeatureListener listener = new FeatureListener() {
      @Override
      public void changed(FeatureEvent featureEvent) {
        if (featureEvent instanceof KafkaFeatureChanged) {
          KafkaFeatureChanged event = ((KafkaFeatureChanged) featureEvent);
          System.out.println("Received add/update for " + event.feature() +
                             " at " + new java.util.Date(event.time()));
        } else if (featureEvent instanceof KafkaFeatureRemoved) {
          KafkaFeatureRemoved event = ((KafkaFeatureRemoved) featureEvent);
          System.out.println("Received delete for " + event.id() + " " + event.feature() +
                             " at " + new java.util.Date(event.time()));
        } else if (featureEvent instanceof KafkaFeatureCleared) {
          KafkaFeatureCleared event = ((KafkaFeatureCleared) featureEvent);
          System.out.println("Received clear at " + new java.util.Date(event.time()));
        }
      }
    };
    store.addFeatureListener(listener);

At cleanup time, it is important to unregister the feature listener with ``removeFeatureListener()``.
For example, for code run in a bean in GeoServer, the ``javax.annotation.PreDestroy`` annotation may
be used to mark the method that does the deregistration:

.. code-block:: java

    @PreDestroy
    public void dispose() throws Exception {
        store.removeFeatureListener(listener);
        // other cleanup
    }

.. _kafka_guaranteed_processing:

Guaranteed Message Processing
=============================

In order to guarantee at-least-once processing of **all** messages, implement an instance of ``GeoMessageProcessor``. The
underlying Kafka consumer will not acknowledge messages until the processor returns, ensuring that they are fully processed
without any errors:

.. tabs::

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.data.KafkaDataStore
        import org.locationtech.geomesa.kafka.utils.GeoMessage;
        import org.locationtech.geomesa.kafka.utils.interop.GeoMessageProcessor;

        GeoMessageProcessor processor = new GeoMessageProcessor() {
            @Override
            public BatchResult consume(List<GeoMessage> records) {
                records.forEach((r) -> {
                    if (r instanceof GeoMessage.Change) {
                        System.out.println(((GeoMessage.Change) r).feature());
                    } else if (r instanceof GeoMessage.Delete) {
                        System.out.println(((GeoMessage.Delete) r).id());
                    } else if (r instanceof GeoMessage.Clear) {
                        System.out.println("clear");
                    }
                });
                return BatchResult.COMMIT;
            }
        };
        // use try-with-resources to close the consumer
        try (((KafkaDataStore) ds).createConsumer(sftName, "my-group-id", processor)) {
          Thread.sleep(10000);
        }

    .. code-tab:: scala

        import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult
        import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult.BatchResult
        import org.locationtech.geomesa.kafka.data.KafkaDataStore
        import org.locationtech.geomesa.kafka.utils.{GeoMessage, GeoMessageProcessor}

        val processor = new GeoMessageProcessor() {
          override def consume(records: Seq[GeoMessage]): BatchResult = {
            records.foreach {
              case GeoMessage.Change(sf) => println(sf)
              case GeoMessage.Delete(id) => println(id)
              case GeoMessage.Clear      => println("clear")
            }
            BatchResult.Commit
          }
        }

        val consumer = ds.asInstanceOf[KafkaDataStore].createConsumer(sftName, "my-group-id", processor)
        try {
          ???
        } finally {
          consumer.close()
        }

.. _FeatureEvent: https://docs.geotools.org/stable/javadocs/org/geotools/api/data/FeatureEvent.html
.. _FeatureEvent.Type: https://docs.geotools.org/stable/javadocs/org/geotools/api/data/FeatureEvent.Type.html
.. _FeatureListener: https://docs.geotools.org/stable/javadocs/org/geotools/api/data/FeatureListener.html
.. _SimpleFeatureSource: https://docs.geotools.org/stable/javadocs/org/geotools/api/data/SimpleFeatureSource.html
