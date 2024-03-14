.. _kafka_layer_views:

Layer Views
-----------

A common use case for the Kafka data store is to expose a single layer with different default filters
and relational transforms to different users. GeoMesa provides a way to do this through configuring
views on top of the original layer.

To enabled layer views, use the ``kafka.layer.views`` data store parameter when creating a store:

.. code-block:: java

    String views =
      "{" +
      "  type1 = [" +
      "    { type-name = \"transformView\", transform = [ \"dtg\", \"geom\", \"name\" ] }," +
      "    { type-name = \"filterView\", filter = \"bbox(geom,0,0,10,10)\" }" +
      "  ]," +
      "  type2 = [" +
      "    { type-name = \"filterTransformView\", filter = \"bbox(geom,0,0,10,10)\", transform = [ \"dtg\", \"geom\" ] }" +
      "  ]" +
      "}";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("kafka.zookeepers", "localhost:2181");
    parameters.put("kafka.brokers", "localhost:9092");
    parameters.put("kafka.layer.views", views);
    org.geotools.api.data.DataStore dataStore =
        org.geotools.api.data.DataStoreFinder.getDataStore(parameters);

The views parameter accepts a TypeSafe config string containing the view definitions. In the above example, assuming
there are existing schemas named ``type1`` and ``type2``, three additional schemas will be exposed with the names
``transformView``, ``filterView`` and ``filterTransformView``. The transform views have a subset of the attributes
from the original schema, while the filter views have a subset of the features in the original schema. Each view
must define either a ``filter``, a ``transform`` or both.
