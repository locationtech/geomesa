.. _kafka_layer_views:

Layer Views
-----------

A common use case for the Kafka data store is to expose a single layer with different default filters
and relational transforms to different users. GeoMesa provides a way to do this through configuring
views on top of the original layer.

To enabled layer views, use the ``kafka.layer.views`` data store parameter when creating a store:

.. code-block:: java

    import org.geotools.data.DataStore;
    import org.geotools.data.DataStoreFinder;

    Map<String, Serializable> parameters = new HashMap<>();
    parameters.put("kafka.zookeepers", "localhost:2181");
    parameters.put("kafka.brokers", "localhost:9092");
    parameters.put("kafka.layer.views",
        "{my-type-name=[{type-name=\"view\",filter=\"INCLUDE\",transform=[\"dtg\",\"geom\"]}]}");
    DataStore dataStore = DataStoreFinder.getDataStore(parameters);

The views parameter accepts a TypeSafe config file containing the view definitions. In the above example, assuming
there is an existing schema named ``my-type-name``, a second schema will be exposed with the name ``view`` and a
subset of the attributes in the original schema. The ``filter`` and ``transform`` keys are optional, but there must
be at least one or the other.
