Listening for Feature Events
----------------------------

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

    import org.geotools.data.FeatureEvent;
    import org.geotools.data.FeatureListener;
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

.. _FeatureEvent: http://docs.geotools.org/stable/javadocs/org/geotools/data/FeatureEvent.html
.. _FeatureEvent.Type: http://docs.geotools.org/stable/javadocs/org/geotools/data/FeatureEvent.Type.html
.. _FeatureListener: http://docs.geotools.org/stable/javadocs/org/geotools/data/FeatureListener.html
.. _SimpleFeatureSource: http://docs.geotools.org/stable/javadocs/org/geotools/data/simple/SimpleFeatureSource.html
