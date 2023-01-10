/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 133afd3681 (GEOMESA-3198 Kafka streams integration (#2854))
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams

<<<<<<< HEAD
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, LongDeserializer, StringDeserializer}
import org.apache.kafka.streams
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, WallclockTimestampExtractor}
import org.apache.kafka.streams.test.TestRecord
=======
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, LongDeserializer, StringDeserializer}
import org.apache.kafka.streams
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.{ProcessorContext, WallclockTimestampExtractor}
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.geotools.api.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
<<<<<<< HEAD
import org.locationtech.geomesa.kafka.KafkaContainerTest
=======
import org.locationtech.geomesa.kafka.EmbeddedKafka
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
<<<<<<< HEAD
=======
import org.specs2.mutable.Specification
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
import org.specs2.runner.JUnitRunner

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, Date, Properties}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
<<<<<<< HEAD
class GeoMesaStreamsBuilderTest extends KafkaContainerTest {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.kstream._
  import org.apache.kafka.streams.scala.serialization.Serdes._
=======
class GeoMesaStreamsBuilderTest extends Specification with StrictLogging {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala.kstream._
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))

  import scala.collection.JavaConverters._

  lazy val sft =
    SimpleFeatureTypes.createImmutableType("streams", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  lazy val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"id$i", s"name$i", i % 2, s"2022-04-27T00:00:0$i.00Z", s"POINT(1 $i)")
  }

<<<<<<< HEAD
=======
  var kafka: EmbeddedKafka = _

  step {
    logger.info("Starting embedded kafka/zk")
    kafka = new EmbeddedKafka()
    logger.info("Started embedded kafka/zk")
  }

>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
  private val zkPaths = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())

  def getParams(zkPath: String): Map[String, String] = {
    require(zkPaths.add(zkPath), s"zk path '$zkPath' is reused between tests, may cause conflicts")
    Map(
<<<<<<< HEAD
      "kafka.brokers"            -> brokers,
      "kafka.zookeepers"         -> zookeepers,
=======
      "kafka.brokers"            -> kafka.brokers,
      "kafka.zookeepers"         -> kafka.zookeepers,
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
      "kafka.topic.partitions"   -> "1",
      "kafka.topic.replication"  -> "1",
      "kafka.consumer.read-back" -> "Inf",
      "kafka.zk.path"            -> zkPath
    )
  }

  "GeoMesaStreamsBuilder" should {
    "read from geomesa topics" in {
      val params = getParams("word/count")
      // write features to the embedded kafka
      val kryoTopic = WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        ds.createSchema(sft)
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        KafkaDataStore.topic(ds.getSchema(sft.getTypeName))
      }

      // read off the features from kafka
      val messages: Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
        val props = new Properties()
<<<<<<< HEAD
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
=======
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokers)
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consume-kryo-topic")
        val buf = ArrayBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
        WithClose(new KafkaConsumer[Array[Byte], Array[Byte]](props)) { consumer =>
          consumer.subscribe(Collections.singleton(kryoTopic))
          val start = System.currentTimeMillis()
          while (buf.lengthCompare(10) < 0 && System.currentTimeMillis() - start < 10000) {
            consumer.poll(Duration.ofMillis(100)).asScala.foreach { record =>
              buf += record
            }
          }
        }
        buf.toSeq
<<<<<<< HEAD
=======
        buf
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
      }

      val timestampExtractor = new TimestampExtractingTransformer()

      val builder = GeoMesaStreamsBuilder(params)

      // pull out the timestamps with an identity transform for later comparison
      val streamFeatures = builder.stream(sft.getTypeName).transform[String, GeoMesaMessage](
        new TransformerSupplier[String, GeoMesaMessage, streams.KeyValue[String, GeoMesaMessage]]() {
          override def get(): Transformer[String, GeoMesaMessage, streams.KeyValue[String, GeoMesaMessage]] =
            timestampExtractor
        })

      // word count example copied from https://kafka.apache.org/31/documentation/streams/developer-guide/dsl-api.html#scala-dsl
      val textLines: KStream[String, String] =
        streamFeatures.map((k, v) => (k, v.attributes.map(_.toString.replaceAll(" ", "_")).mkString(" ")))
      val wordCounts: KTable[String, Long] =
        textLines
            .flatMapValues(textLine => textLine.split(" +"))
            .groupBy((_, word) => word)
            .count()(Materialized.as("counts-store"))

      wordCounts.toStream.to("word-count")

      val props = new Properties()
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-test")
<<<<<<< HEAD
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

      val output = scala.collection.mutable.Map.empty[String, java.lang.Long]
      WithClose(new TopologyTestDriver(builder.build(), props)) { testDriver =>
<<<<<<< HEAD
        val inputTopic = testDriver.createInputTopic(kryoTopic, new ByteArraySerializer(), new ByteArraySerializer())
        messages.foreach(m => inputTopic.pipeInput(new TestRecord[Array[Byte],Array[Byte]](m)))
        val outputTopic = testDriver.createOutputTopic("word-count", new StringDeserializer(), new LongDeserializer())
        while (!outputTopic.isEmpty) {
          val rec = outputTopic.readRecord
          output += rec.key() -> rec.value()
=======
        messages.foreach(testDriver.pipeInput)
        var out = testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer())
        while (out != null) {
          output += out.key() -> out.value()
          out = testDriver.readOutput("word-count", new StringDeserializer(), new LongDeserializer())
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
        }
      }

      val expected =
        features.flatMap(_.getAttributes.asScala.map(_.toString.replaceAll(" ", "_")))
            .groupBy(s => s)
            .map { case (k, v) => k -> v.length.toLong }

      output mustEqual expected

      val expectedTimestamps = features.map(_.getAttribute("dtg").asInstanceOf[Date].getTime)
      foreach(timestampExtractor.timestamps)(_._2 must haveLength(1))
      timestampExtractor.timestamps.map(_._2.head) must containTheSameElementsAs(expectedTimestamps)
    }

    "write to geomesa topics" in {
      val params = getParams("write/test")
      // create the output feature type and topic
      val kryoTopic = WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
        ds.createSchema(sft)
        KafkaDataStore.topic(ds.getSchema(sft.getTypeName))
      }

      val testInput = features.map { sf =>
        val offset = sf.getID.replace("id", "").toLong
        val key = sf.getID.getBytes(StandardCharsets.UTF_8)
        val value = sf.getAttributes.asScala.map(FastConverter.convert(_, classOf[String])).mkString(",")
        new ConsumerRecord("input-topic", 0, offset, key, value.getBytes(StandardCharsets.UTF_8))
      }

      val builder = GeoMesaStreamsBuilder(params)

      val input: KStream[String, String] =
        builder.wrapped.stream[String, String]("input-topic")(implicitly[Consumed[String, String]].withTimestampExtractor(new WallclockTimestampExtractor()))
      val output: KStream[String, GeoMesaMessage] =
        input.mapValues { lines =>
          GeoMesaMessage.upsert(lines.split(","))
        }

      builder.to(sft.getTypeName, output)

      val props = new Properties()
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "write-test")
<<<<<<< HEAD
=======
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geomesa-test-app")
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

      val kryoMessages = ArrayBuffer.empty[ProducerRecord[Array[Byte], Array[Byte]]]
      WithClose(new TopologyTestDriver(builder.build(), props)) { testDriver =>
<<<<<<< HEAD
        val inputTopic = testDriver.createInputTopic("input-topic", new ByteArraySerializer(), new ByteArraySerializer())
        testInput.foreach(m => inputTopic.pipeInput(new TestRecord[Array[Byte],Array[Byte]](m)))
        val outputTopic = testDriver.createOutputTopic(kryoTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        while (!outputTopic.isEmpty) {
          val rec = outputTopic.readRecord
          kryoMessages += new ProducerRecord(kryoTopic, 0, rec.timestamp, rec.getKey, rec.getValue, rec.getHeaders)
=======
        testInput.foreach(testDriver.pipeInput)
        var out = testDriver.readOutput(kryoTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        while (out != null) {
          kryoMessages += out
          out = testDriver.readOutput(kryoTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
        }
      }

      WithClose(DataStoreFinder.getDataStore(params.asJava)) { case ds: KafkaDataStore =>
        // initialize kafka consumers for the store
        ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT).close()
        // send the mocked messages to the actual embedded kafka topic
        WithClose(KafkaDataStore.producer(ds.config.brokers, Map.empty)) { producer =>
          kryoMessages.foreach(producer.send)
        }
        eventually(40, 100.millis) {
          val result = SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList
          result must containTheSameElementsAs(features)
        }
      }
    }
  }

<<<<<<< HEAD
=======
  step {
    logger.info("Stopping embedded kafka/zk")
    kafka.close()
    logger.info("Stopped embedded kafka/zk")
  }

>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
  class TimestampExtractingTransformer
      extends Transformer[String, GeoMesaMessage, org.apache.kafka.streams.KeyValue[String, GeoMesaMessage]]() {

    private var context: ProcessorContext = _

    val timestamps = scala.collection.mutable.Map.empty[String, ArrayBuffer[Long]]

    override def init(context: ProcessorContext): Unit = this.context = context

    override def transform(key: String, value: GeoMesaMessage): streams.KeyValue[String, GeoMesaMessage] = {
      timestamps.getOrElseUpdate(key, ArrayBuffer.empty) += context.timestamp()
      new streams.KeyValue(key, value)
    }

    override def close(): Unit = {}
  }

}
