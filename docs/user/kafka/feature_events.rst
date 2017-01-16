Listening for Feature Events
----------------------------

The GeoTools API includes a mechanism to fire off a `FeatureEvent`_ object each time
that there is an "event," which occurs when data are added, changed, or deleted in a
`SimpleFeatureSource`_. A client may implement a `FeatureListener`_, which has a single
method called ``changed()`` that is invoked each time that each `FeatureEvent`_ is
fired.

Three types of messages are produced by a GeoMesa Kafka producer, which can be read
by a GeoMesa Kafka consumer. For a live consumer feature source, reading each of these
messages causes a `FeatureEvent`_ to be fired, as summarized in the table below:

+----------------+------------------------------------+-----------------------+----------------------+--------------------+
| Message read   | Meaning                            | Class of event fired  | `FeatureEvent.Type`_ | Filter             |
+================+====================================+=======================+======================+====================+
| CreateOrUpdate | A single feature with a given id   | ``KafkaFeatureEvent`` | ``CHANGED``          | ``IN (<id>)``      |
|                | has been added; this               |                       |                      |                    |
|                | may be a new feature, or an update |                       |                      |                    |
|                | of an existing feature.            |                       |                      |                    |
+----------------+------------------------------------+-----------------------+----------------------+--------------------+
| Delete         | The feature with the given id      | ``FeatureEvent``      | ``REMOVED``          | ``IN (<id>)``      |
|                | has been removed.                  |                       |                      |                    |
+----------------+------------------------------------+-----------------------+----------------------+--------------------+
| Clear          | All features have been removed.    | ``FeatureEvent``      | ``REMOVED``          | ``Filter.INCLUDE`` |
+----------------+------------------------------------+-----------------------+----------------------+--------------------+

For a CreateOrUpdate message, the `FeatureEvent`_ returned is actually a ``KafkaFeatureEvent``,
which subclasses `FeatureEvent`_. ``KafkaFeatureEvent`` adds a ``feature()`` method, which returns
the ``SimpleFeature`` added or updated.

To register a `FeatureListener`_, create the `SimpleFeatureSource`_ from a live GeoMesa
Kafka consumer data store, and use the ``addFeatureListener()`` method. For example, the
following listener simply prints out the events it receives:

.. code-block:: java

    // the live consumer must be created before the producer writes features
    // in order to read streaming data.
    // i.e. the live consumer will only read data written after its instantiation
    SimpleFeatureSource consumerFS = consumerDS.getFeatureSource(sftName);
    FeatureListener listener = new FeatureListener() {
        @Override
        public void changed(FeatureEvent featureEvent) {
            System.out.println("Received FeatureEvent of Type: " + featureEvent.getType());

            if (featureEvent.getType() == FeatureEvent.Type.CHANGED &&
                    featureEvent instanceof KafkaFeatureEvent) {
                SimpleFeature feature = ((KafkaFeatureEvent) featureEvent).feature();
                System.out.println(feature);
            }

            if (featureEvent.getType() == FeatureEvent.Type.REMOVED) {
                System.out.println("Received Delete for filter: " + featureEvent.getFilter());
            }
        }
    }
    consumerFS.addFeatureListener(listener);

At cleanup time, it is important to unregister the feature listener with ``removeFeatureListener()``.
For example, for code run in a bean in GeoServer, the ``javax.annotation.PreDestroy`` annotation may
be used to mark the method that does the deregistration:

.. code-block:: java

    @PreDestroy
    public void dispose() throws Exception {
        consumerFS.removeFeatureListener(listener);
        // other cleanup
    }

.. _FeatureEvent: http://docs.geotools.org/stable/javadocs/org/geotools/data/FeatureEvent.html
.. _FeatureEvent.Type: http://docs.geotools.org/stable/javadocs/org/geotools/data/FeatureEvent.Type.html
.. _FeatureListener: http://docs.geotools.org/stable/javadocs/org/geotools/data/FeatureListener.html
.. _SimpleFeatureSource: http://docs.geotools.org/stable/javadocs/org/geotools/data/simple/SimpleFeatureSource.html