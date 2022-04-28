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

Write Example
-------------

The following shows how to persist data back to a GeoMesa topic:

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
