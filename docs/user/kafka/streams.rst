.. _streams_kds:

Kafka Streams Integration
=========================

The Kafka Data Store can integrate with Kafka Streams applications as both a source and a destination.

To read from or write to a GeoMesa Kafka topic, use the ``org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder``
class. This class wraps a regular Kafka ``StreamsBuilder`` with convenience methods for mapping feature types to
topics and loading serializers.

Streams Data Model
------------------

Messages are based on SimpleFeatures and modelled as a simple case class:

.. code-block:: scala

  case class GeoMesaMessage(action: MessageAction, attributes: Seq[AnyRef], userData: Map[String, String])

  object MessageAction extends Enumeration {
    type MessageAction = Value
    val Upsert, Delete = Value
  }

Read Example
------------

The following is adapted from the word-count `example`__ in Kafka Streams:

__ https://kafka.apache.org/31/documentation/streams/developer-guide/dsl-api.html#scala-dsl-sample-usage

<<<<<<< HEAD
.. tabs::
<<<<<<< HEAD

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // read in the feature type and turn the attributes into a space-separated string
        val textLines: KStream[String, String] =
          builder.stream("my-feature-type")
              .map((k, v) => (k, v.attributes.map(_.toString.replaceAll(" ", "_")).mkString(" ")))

        val wordCounts: KTable[String, Long] =
          textLines
              .flatMapValues(textLine => textLine.split(" +"))
              .groupBy((_, word) => word)
              .count()(Materialized.as("counts-store"))

        wordCounts.toStream.to("word-count")

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // read in the feature type and turn the attributes into a space-separated string
        KStream<String, String> textLines =
              builder.stream("my-feature-type")
                     .mapValues(v -> v.asJava().stream().map(Object::toString).collect(Collectors.joining(" ")));

        KTable<String, Long> wordCounts =
              textLines
                    .flatMapValues(textLine -> Arrays.asList(textLine.split(" +")))
                    .groupBy((k, word) -> word)
                    .count(Materialized.as("counts-store"));

        wordCounts.toStream().to("word-count", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        // construct the streams app as normal

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 150c5dbc25 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
.. code-block:: scala
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // read in the feature type and turn the attributes into a space-separated string
        val textLines: KStream[String, String] =
          builder.stream("my-feature-type")
              .map((k, v) => (k, v.attributes.map(_.toString.replaceAll(" ", "_")).mkString(" ")))

        val wordCounts: KTable[String, Long] =
          textLines
              .flatMapValues(textLine => textLine.split(" +"))
              .groupBy((_, word) => word)
              .count()(Materialized.as("counts-store"))

        wordCounts.toStream.to("word-count")

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // read in the feature type and turn the attributes into a space-separated string
        KStream<String, String> textLines =
              builder.stream("my-feature-type")
                     .mapValues(v -> v.asJava().stream().map(Object::toString).collect(Collectors.joining(" ")));

        KTable<String, Long> wordCounts =
              textLines
                    .flatMapValues(textLine -> Arrays.asList(textLine.split(" +")))
                    .groupBy((k, word) -> word)
                    .count(Materialized.as("counts-store"));

        wordCounts.toStream().to("word-count", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        // construct the streams app as normal

<<<<<<< HEAD
    val topology = builder.build()
    // construct the streams app as normal
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
.. code-block:: scala
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // read in the feature type and turn the attributes into a space-separated string
        val textLines: KStream[String, String] =
          builder.stream("my-feature-type")
              .map((k, v) => (k, v.attributes.map(_.toString.replaceAll(" ", "_")).mkString(" ")))

        val wordCounts: KTable[String, Long] =
          textLines
              .flatMapValues(textLine => textLine.split(" +"))
              .groupBy((_, word) => word)
              .count()(Materialized.as("counts-store"))

        wordCounts.toStream.to("word-count")

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // read in the feature type and turn the attributes into a space-separated string
        KStream<String, String> textLines =
              builder.stream("my-feature-type")
                     .mapValues(v -> v.asJava().stream().map(Object::toString).collect(Collectors.joining(" ")));

        KTable<String, Long> wordCounts =
              textLines
                    .flatMapValues(textLine -> Arrays.asList(textLine.split(" +")))
                    .groupBy((k, word) -> word)
                    .count(Materialized.as("counts-store"));

        wordCounts.toStream().to("word-count", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        // construct the streams app as normal

<<<<<<< HEAD
    val topology = builder.build()
    // construct the streams app as normal
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
=======
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
.. code-block:: scala
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // read in the feature type and turn the attributes into a space-separated string
        val textLines: KStream[String, String] =
          builder.stream("my-feature-type")
              .map((k, v) => (k, v.attributes.map(_.toString.replaceAll(" ", "_")).mkString(" ")))

        val wordCounts: KTable[String, Long] =
          textLines
              .flatMapValues(textLine => textLine.split(" +"))
              .groupBy((_, word) => word)
              .count()(Materialized.as("counts-store"))

        wordCounts.toStream.to("word-count")

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // read in the feature type and turn the attributes into a space-separated string
        KStream<String, String> textLines =
              builder.stream("my-feature-type")
                     .mapValues(v -> v.asJava().stream().map(Object::toString).collect(Collectors.joining(" ")));

        KTable<String, Long> wordCounts =
              textLines
                    .flatMapValues(textLine -> Arrays.asList(textLine.split(" +")))
                    .groupBy((k, word) -> word)
                    .count(Materialized.as("counts-store"));

        wordCounts.toStream().to("word-count", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        // construct the streams app as normal

<<<<<<< HEAD
    val topology = builder.build()
    // construct the streams app as normal
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 7258020868 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
.. code-block:: scala
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // read in the feature type and turn the attributes into a space-separated string
        val textLines: KStream[String, String] =
          builder.stream("my-feature-type")
              .map((k, v) => (k, v.attributes.map(_.toString.replaceAll(" ", "_")).mkString(" ")))

        val wordCounts: KTable[String, Long] =
          textLines
              .flatMapValues(textLine => textLine.split(" +"))
              .groupBy((_, word) => word)
              .count()(Materialized.as("counts-store"))

        wordCounts.toStream.to("word-count")

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // read in the feature type and turn the attributes into a space-separated string
        KStream<String, String> textLines =
              builder.stream("my-feature-type")
                     .mapValues(v -> v.asJava().stream().map(Object::toString).collect(Collectors.joining(" ")));

        KTable<String, Long> wordCounts =
              textLines
                    .flatMapValues(textLine -> Arrays.asList(textLine.split(" +")))
                    .groupBy((k, word) -> word)
                    .count(Materialized.as("counts-store"));

        wordCounts.toStream().to("word-count", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        // construct the streams app as normal

<<<<<<< HEAD
    val topology = builder.build()
    // construct the streams app as normal
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> dfd160ebf9 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 150c5dbc25 (GEOMESA-3198 Kafka streams integration (#2854))
.. code-block:: scala

    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._
    import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

    // these are the parameters normally passed to DataStoreFinder
    val params = Map[String, String](
      "kafka.brokers"    -> "localhost:9092",
      "kafka.zookeepers" -> "localhost:2181"
    )

    val builder = GeoMesaStreamsBuilder(params)

    // read in the feature type and turn the attributes into a space-separated string
    val textLines: KStream[String, String] =
      builder.stream("my-feature-type")
          .map((k, v) => (k, v.attributes.map(_.toString.replaceAll(" ", "_")).mkString(" ")))

    val wordCounts: KTable[String, Long] =
      textLines
          .flatMapValues(textLine => textLine.split(" +"))
          .groupBy((_, word) => word)
          .count()(Materialized.as("counts-store"))

    wordCounts.toStream.to("word-count")

    val topology = builder.build()
    // construct the streams app as normal
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> location-main
=======
<<<<<<< HEAD
>>>>>>> 150c5dbc25 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> locationtech-main
=======
>>>>>>> dfd160ebf9 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 150c5dbc25 (GEOMESA-3198 Kafka streams integration (#2854))

Write Example
-------------

The following shows how to persist data back to a GeoMesa topic:

<<<<<<< HEAD
.. tabs::
<<<<<<< HEAD

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // use the wrapped native streams builder to create an input based on csv records
        val input: KStream[String, String] =
          builder.wrapped.stream[String, String]("input-csv-topic")

        // the columns in the csv need to map to the attributes in the feature type
        val output: KStream[String, GeoMesaMessage] =
          input.mapValues(lines => GeoMesaMessage.upsert(lines.split(",")))

        // write the output to GeoMesa - the feature type must already exist
        builder.to("my-feature-type", output)

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // use the wrapped native streams builder to create an input based on csv records
        KStream<String, String> input =
            builder.wrapped().stream("input-topic",
                Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new WallclockTimestampExtractor()));
        // the columns in the csv need to map to the attributes in the feature type
        KStream<String, GeoMesaMessage> output =
            input.mapValues(lines -> GeoMesaMessage.upsert(Arrays.asList(lines.split(","))));

        // write the output to GeoMesa - the feature type must already exist
        builder.to(sft.getTypeName(), output);

        Topology topology = builder.build();
        // construct the streams app as normal
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 150c5dbc25 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
.. code-block:: scala
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // use the wrapped native streams builder to create an input based on csv records
        val input: KStream[String, String] =
          builder.wrapped.stream[String, String]("input-csv-topic")

        // the columns in the csv need to map to the attributes in the feature type
        val output: KStream[String, GeoMesaMessage] =
          input.mapValues(lines => GeoMesaMessage.upsert(lines.split(",")))

<<<<<<< HEAD
    val topology = builder.build()
    // construct the streams app as normal
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
        // write the output to GeoMesa - the feature type must already exist
        builder.to("my-feature-type", output)

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // use the wrapped native streams builder to create an input based on csv records
        KStream<String, String> input =
            builder.wrapped().stream("input-topic",
                Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new WallclockTimestampExtractor()));
        // the columns in the csv need to map to the attributes in the feature type
        KStream<String, GeoMesaMessage> output =
            input.mapValues(lines -> GeoMesaMessage.upsert(Arrays.asList(lines.split(","))));

        // write the output to GeoMesa - the feature type must already exist
        builder.to(sft.getTypeName(), output);

        Topology topology = builder.build();
        // construct the streams app as normal
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
.. code-block:: scala
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // use the wrapped native streams builder to create an input based on csv records
        val input: KStream[String, String] =
          builder.wrapped.stream[String, String]("input-csv-topic")

        // the columns in the csv need to map to the attributes in the feature type
        val output: KStream[String, GeoMesaMessage] =
          input.mapValues(lines => GeoMesaMessage.upsert(lines.split(",")))

<<<<<<< HEAD
    val topology = builder.build()
    // construct the streams app as normal
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
        // write the output to GeoMesa - the feature type must already exist
        builder.to("my-feature-type", output)

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // use the wrapped native streams builder to create an input based on csv records
        KStream<String, String> input =
            builder.wrapped().stream("input-topic",
                Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new WallclockTimestampExtractor()));
        // the columns in the csv need to map to the attributes in the feature type
        KStream<String, GeoMesaMessage> output =
            input.mapValues(lines -> GeoMesaMessage.upsert(Arrays.asList(lines.split(","))));

        // write the output to GeoMesa - the feature type must already exist
        builder.to(sft.getTypeName(), output);

        Topology topology = builder.build();
        // construct the streams app as normal
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
=======
=======
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
.. code-block:: scala
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // use the wrapped native streams builder to create an input based on csv records
        val input: KStream[String, String] =
          builder.wrapped.stream[String, String]("input-csv-topic")

        // the columns in the csv need to map to the attributes in the feature type
        val output: KStream[String, GeoMesaMessage] =
          input.mapValues(lines => GeoMesaMessage.upsert(lines.split(",")))

<<<<<<< HEAD
    val topology = builder.build()
    // construct the streams app as normal
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 7258020868 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
        // write the output to GeoMesa - the feature type must already exist
        builder.to("my-feature-type", output)

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // use the wrapped native streams builder to create an input based on csv records
        KStream<String, String> input =
            builder.wrapped().stream("input-topic",
                Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new WallclockTimestampExtractor()));
        // the columns in the csv need to map to the attributes in the feature type
        KStream<String, GeoMesaMessage> output =
            input.mapValues(lines -> GeoMesaMessage.upsert(Arrays.asList(lines.split(","))));

        // write the output to GeoMesa - the feature type must already exist
        builder.to(sft.getTypeName(), output);

        Topology topology = builder.build();
        // construct the streams app as normal
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
.. code-block:: scala
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    .. code-tab:: scala

        import org.apache.kafka.streams.scala.ImplicitConversions._
        import org.apache.kafka.streams.scala.serialization.Serdes._
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage
        import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

        // these are the parameters normally passed to DataStoreFinder
        val params = Map[String, String](
          "kafka.brokers"    -> "localhost:9092",
          "kafka.zookeepers" -> "localhost:2181"
        )

        val builder = GeoMesaStreamsBuilder(params)

        // use the wrapped native streams builder to create an input based on csv records
        val input: KStream[String, String] =
          builder.wrapped.stream[String, String]("input-csv-topic")

        // the columns in the csv need to map to the attributes in the feature type
        val output: KStream[String, GeoMesaMessage] =
          input.mapValues(lines => GeoMesaMessage.upsert(lines.split(",")))

<<<<<<< HEAD
    val topology = builder.build()
    // construct the streams app as normal
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
        // write the output to GeoMesa - the feature type must already exist
        builder.to("my-feature-type", output)

        val topology = builder.build()
        // construct the streams app as normal

    .. code-tab:: java

        import org.locationtech.geomesa.kafka.jstreams.GeoMesaStreamsBuilder;
        import org.locationtech.geomesa.kafka.streams.GeoMesaMessage;

        // these are the parameters normally passed to DataStoreFinder
        Map<String, String> params = new HashMap<>();
        params.put("kafka.brokers", "localhost:9092");
        params.put("kafka.zookeepers", "localhost:2181");

        GeoMesaStreamsBuilder builder = GeoMesaStreamsBuilder.create(params)

        // use the wrapped native streams builder to create an input based on csv records
        KStream<String, String> input =
            builder.wrapped().stream("input-topic",
                Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new WallclockTimestampExtractor()));
        // the columns in the csv need to map to the attributes in the feature type
        KStream<String, GeoMesaMessage> output =
            input.mapValues(lines -> GeoMesaMessage.upsert(Arrays.asList(lines.split(","))));

        // write the output to GeoMesa - the feature type must already exist
        builder.to(sft.getTypeName(), output);

        Topology topology = builder.build();
        // construct the streams app as normal
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> dfd160ebf9 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 150c5dbc25 (GEOMESA-3198 Kafka streams integration (#2854))
.. code-block:: scala

    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.serialization.Serdes._
    import org.locationtech.geomesa.kafka.streams.GeoMesaMessage
    import org.locationtech.geomesa.kafka.streams.GeoMesaStreamsBuilder

    // these are the parameters normally passed to DataStoreFinder
    val params = Map[String, String](
      "kafka.brokers"    -> "localhost:9092",
      "kafka.zookeepers" -> "localhost:2181"
    )

    val builder = GeoMesaStreamsBuilder(params)

    // use the wrapped native streams builder to create an input based on csv records
    val input: KStream[String, String] =
      builder.builder.stream[String, String]("input-csv-topic")

    // the columns in the csv need to map to the attributes in the feature type
    val output: KStream[String, GeoMesaMessage] =
      input.mapValues(lines => GeoMesaMessage.upsert(lines.split(",")))

    // write the output to GeoMesa - the feature type must already exist
    builder.to("my-feature-type", output)

    val topology = builder.build()
    // construct the streams app as normal
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> location-main
=======
<<<<<<< HEAD
>>>>>>> 150c5dbc25 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> locationtech-main
=======
>>>>>>> dfd160ebf9 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 150c5dbc25 (GEOMESA-3198 Kafka streams integration (#2854))

Joins and Topic Partitioning
----------------------------

.. warning::

    Kafka streams operations that require co-partitioning will usually require a re-partition step when used
    with data from GeoMesa 3.4.x or earlier.

For feature types created prior to version 3.5.0, GeoMesa will use a custom partitioner for data
written to Kafka. When using Kafka Streams operations that require co-partitioning, such as joins, the
GeoMesa topic will need to be re-partitioned.

Existing feature types can be updated to use the default Kafka partitioner, which will allow for joins without
re-partitioning. However, note that updates for a given feature may go to the wrong partition, which may result
in older data being returned when GeoMesa is queried. This will generally resolve itself over time as Kafka log
retention policies delete the older data (unless Kafka log compaction is enabled for the topic).

To update an existing feature type, add the user data entry ``geomesa.kafka.partitioning=default``. Through the
GeoMesa command-line tools, the following command will disable custom partitioning:

.. code-block:: bash

  geomesa-kafka update-schema --add-user-data "geomesa.kafka.partitioning:default"
