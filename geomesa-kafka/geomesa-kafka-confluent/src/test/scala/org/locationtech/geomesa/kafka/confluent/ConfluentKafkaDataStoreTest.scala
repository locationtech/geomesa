/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import java.util.{Date, Properties}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.geotools.data._
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeomesaAvroDateFormat, GeomesaAvroFeatureVisibility, GeomesaAvroGeomDefault, GeomesaAvroGeomFormat, GeomesaAvroGeomType, GeomesaAvroProperty}
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, GeometryFactory, Point}
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory
import org.specs2.mutable.{After, Specification}
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ConfluentKafkaDataStoreTest extends Specification with LazyLogging {
  sequential

  "ConfluentKafkaDataStore" should {

    val topic = "confluent-test"

    val schemaJson =
      s"""{
         |  "type":"record",
         |  "name":"schema",
         |  "fields":[
         |    {
         |      "name":"position",
         |      "type":"string",
         |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKT}",
         |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.POINT}",
         |      "${GeomesaAvroGeomDefault.KEY}":"true"
         |    },
         |    {
         |      "name":"speed",
         |      "type":"double"
         |    },
         |    {
         |      "name":"date",
         |      "type":"string",
         |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.ISO_INSTANT}"
         |    },
         |    {
         |      "name":"visibility",
         |      "type":"string",
         |      "${GeomesaAvroFeatureVisibility.KEY}":""
         |    }
         |  ]
         |}""".stripMargin
    val schema = new Schema.Parser().parse(schemaJson)

    /*
    "fail to get a feature source when the schema is invalid" in new ConfluentKafkaTestContext {
      private val badSchemaJson =
        s"""{
          |  "type":"record",
          |  "name":"schema",
          |  "fields":[
          |    {
          |      "name":"f1",
          |      "type":"string",
          |      "${GeomesaAvroGeomFormat.KEY}":"bad-format",
          |      "${GeomesaAvroGeomType.KEY}":"bad-type"
          |    }
          |  ]
          |}""".stripMargin
      private val badSchema = new Schema.Parser().parse(badSchemaJson)

      private val record = new GenericData.Record(badSchema)
      record.put("f1", "POINT(10 20)")

      private val producer = getProducer
      producer.send(new ProducerRecord[String, GenericRecord](topic, null, record))

      private val kds = getStore

      kds.getFeatureSource(topic) must throwAn[Exception]
    }
     */

    "Deserialize simple features when the schema and records are valid" in new ConfluentKafkaTestContext {
      private val id = "1"
      private val record = new GenericData.Record(schema)
      record.put("position", "POINT(10 20)")
      record.put("speed", 12.325d)
      record.put("date", "2021-12-07T17:22:24.897-05:00")
      record.put("visibility", "visible")

      private val producer = getProducer
      producer.send(new ProducerRecord[String, GenericRecord](topic, id, record)).get()

      private val kds = getStore
      private val fs = kds.getFeatureSource(topic)

      eventually(40, 100.millis) {
        SelfClosingIterator(fs.getFeatures.features()).toArray.length mustEqual 1

        val feature = fs.getFeatures.features().next()

        val expectedPosition = generateCoordinate(10, 20)
        feature.getID mustEqual "1"
        feature.getAttribute("position") mustEqual expectedPosition
        feature.getAttribute("speed") mustEqual 12.325d
        feature.getAttribute("date") mustEqual new Date(1638915744897L)
        feature.getAttribute("visibility") mustEqual "visible"
        feature.getDefaultGeometry mustEqual expectedPosition
        SecurityUtils.getVisibility(feature) mustEqual "visible"
      }
    }

    /*
    "support the GeoMessage control operation for" >> {
      "update" in new ConfluentKafkaTestContext {

      }

      "drop" in new ConfluentKafkaTestContext {

      }

      "clear" in new ConfluentKafkaTestContext {

      }
    }
     */
  }

  private lazy val geomFactory = new GeometryFactory()
  private lazy val coordinateFactory = CoordinateArraySequenceFactory.instance()

  private def generateCoordinate(x: Double, y: Double): CoordinateSequence = {
    coordinateFactory.create(Array(new Coordinate(x, y)))
  }

  /*
  "ConfluentKafkaDataStore" should {
    "read schemas from confluent schema registry" >> {
      val topic = "confluent-test"
      val producerProps = new Properties()
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, confluentKafka.brokers)
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
      producerProps.put("schema.registry.url", confluentKafka.schemaRegistryUrl)
      val producer = new KafkaProducer[String, GenericRecord](producerProps)

      val key = "confluentTestKey"
      val userSchema =
        """{
          |  "type":"record",
          |  "name":"myrecord",
          |  "fields":[
          |    {"name":"f1","type":["null","string"]},
          |    {"name":"f2","type":"string"}
          |  ]
          |}""".stripMargin
      val schema = new Schema.Parser().parse(userSchema)
      val avroRecord = new GenericData.Record(schema)
      avroRecord.put("f1", "value1")
      avroRecord.put("f2", "POINT(10 20)")

      producer.send(new ProducerRecord[String, GenericRecord](topic, 0, 1540486908L, key, avroRecord))

      val kds = getStore()
      val fs = kds.getFeatureSource(topic) // start the consumer polling
      eventually(40, 100.millis)(SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 1)
      val feature = fs.getFeatures.features.next()
      feature.getID mustEqual key
      feature.getAttribute("f1") mustEqual "value1"
      feature.getAttribute("f2") mustEqual "POINT(10 20)"
      feature.getAttribute(ConfluentFeatureSerializer.DateAttributeName).asInstanceOf[Date].getTime mustEqual 1540486908L
      val point = feature.getAttribute(ConfluentFeatureSerializer.GeomAttributeName).asInstanceOf[Point]
      point.getX mustEqual 10
      point.getY mustEqual 20
    }
  }
   */
}

trait ConfluentKafkaTestContext extends After {

  val confluentKafka = new EmbeddedConfluent()

  def getStore: KafkaDataStore = {
    val baseParams = Map(
      "kafka.schema.registry.url" -> confluentKafka.schemaRegistryUrl,
      "kafka.brokers"             -> confluentKafka.brokers,
      "kafka.zookeepers"          -> confluentKafka.zookeepers,
      "kafka.topic.partitions"    -> 1,
      "kafka.topic.replication"   -> 1,
      "kafka.consumer.read-back"  -> "Inf",
      "kafka.zk.path"             -> "geomesa/ds/kafka",
      "kafka.consumer.count"      -> 1
    )

    DataStoreFinder.getDataStore(baseParams).asInstanceOf[KafkaDataStore]
  }

  def getProducer: KafkaProducer[String, GenericRecord] = {
    val producerProps = new Properties()

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, confluentKafka.brokers)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    producerProps.put("schema.registry.url", confluentKafka.schemaRegistryUrl)

    new KafkaProducer[String, GenericRecord](producerProps)
  }

  override def after: Any = confluentKafka.close()
}
