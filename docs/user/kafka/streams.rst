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

.. tabs::

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


Write Example
-------------

The following shows how to persist data back to a GeoMesa topic:

.. tabs::

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
