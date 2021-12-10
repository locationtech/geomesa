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
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeomesaAvroDateFormat, GeomesaAvroFeatureVisibility, GeomesaAvroGeomDefault, GeomesaAvroGeomFormat, GeomesaAvroGeomType}
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}
import org.specs2.mutable.{After, Specification}
import org.specs2.runner.JUnitRunner

import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ConfluentKafkaDataStoreTest extends Specification with LazyLogging {
  sequential

  "ConfluentKafkaDataStore" should {

    val topic = "confluent-kds-test"

    val schemaJson1 =
      s"""{
         |  "type":"record",
         |  "name":"schema1",
         |  "fields":[
         |    {
         |      "name":"position",
         |      "type":"string",
         |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKT}",
         |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.POINT}",
         |      "${GeomesaAvroGeomDefault.KEY}":"${GeomesaAvroGeomDefault.TRUE}"
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
         |      "${GeomesaAvroFeatureVisibility.KEY}":"${GeomesaAvroFeatureVisibility.VALUE}"
         |    }
         |  ]
         |}""".stripMargin
    val schema1 = new Schema.Parser().parse(schemaJson1)

    val schemaJson2 =
      s"""{
         |  "type":"record",
         |  "name":"schema2",
         |  "fields":[
         |    {
         |      "name":"shape",
         |      "type":"bytes",
         |      "${GeomesaAvroGeomFormat.KEY}":"${GeomesaAvroGeomFormat.WKB}",
         |      "${GeomesaAvroGeomType.KEY}":"${GeomesaAvroGeomType.GEOMETRY}",
         |      "${GeomesaAvroGeomDefault.KEY}":"${GeomesaAvroGeomDefault.TRUE}"
         |    },
         |    {
         |      "name":"date",
         |      "type":["null","long"],
         |      "${GeomesaAvroDateFormat.KEY}":"${GeomesaAvroDateFormat.EPOCH_MILLIS}"
         |    }
         |  ]
         |}""".stripMargin
    val schema2 = new Schema.Parser().parse(schemaJson2)

    "fail to get a feature source when the schema cannot be converted to an SFT" in new ConfluentKafkaTestContext {
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
      producer.send(new ProducerRecord[String, GenericRecord](topic, null, record)).get

      private val kds = getStore

      kds.getFeatureSource(topic) must throwAn[Exception]
    }

    "deserialize simple features when the schema and records are valid" in new ConfluentKafkaTestContext {
      private val id = "1"
      private val record = new GenericData.Record(schema1)
      record.put("position", "POINT(10 20)")
      record.put("speed", 12.325d)
      record.put("date", "2021-12-07T17:22:24.897-05:00")
      record.put("visibility", "") // no auths are configured so this should be empty else the feature will be hidden

      private val producer = getProducer
      producer.send(new ProducerRecord[String, GenericRecord](topic, id, record)).get

      private val kds = getStore
      private val fs = kds.getFeatureSource(topic)

      eventually(10, 100.millis) {
        SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 1

        val feature = fs.getFeatures.features.next
        val expectedPosition = generatePoint(10d, 20d)
        feature.getID mustEqual id
        feature.getAttribute("position") mustEqual expectedPosition
        feature.getAttribute("speed") mustEqual 12.325d
        feature.getAttribute("date") mustEqual new Date(1638915744897L)
        feature.getAttribute("visibility") mustEqual ""
        feature.getDefaultGeometry mustEqual expectedPosition
        SecurityUtils.getVisibility(feature) mustEqual ""
      }
    }

    "support the GeoMessage control operation for" >> {
      "update" in new ConfluentKafkaTestContext {
        private val id = "1"
        private val record1 = new GenericData.Record(schema2)
        private val expectedGeom1 = generatePoint(10d, 20d)
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", 1639145281285L)

        private val producer = getProducer
        producer.send(new ProducerRecord[String, GenericRecord](topic, id, record1)).get

        private val kds = getStore
        private val fs = kds.getFeatureSource(topic)

        eventually(10, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 1

          val feature = fs.getFeatures.features.next
          feature.getID mustEqual id
          feature.getAttribute("shape") mustEqual expectedGeom1
          feature.getAttribute("date") mustEqual new Date(1639145281285L)
        }

        private val record2 = new GenericData.Record(schema2)
        private val expectedGeom2 = generatePoint(15d, 30d)
        record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
        record2.put("date", null)

        producer.send(new ProducerRecord[String, GenericRecord](topic, id, record2)).get

        eventually(10, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 1

          val feature = fs.getFeatures.features.next
          feature.getID mustEqual id
          feature.getAttribute("shape") mustEqual expectedGeom2
          feature.getAttribute("date") mustEqual null
        }
      }

      "drop" in new ConfluentKafkaTestContext {
        private val id1 = "1"
        private val record1 = new GenericData.Record(schema2)
        private val expectedGeom1 = generatePolygon(Seq((0d,0d), (10d,0d), (10d,10d), (0d,10d), (0d,0d)))
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", 1639145281285L)

        private val id2 = "2"
        private val record2 = new GenericData.Record(schema2)
        private val expectedGeom2 = generatePolygon(Seq((5d,5d), (15d,5d), (15d,15d), (5d,15d), (5d,5d)))
        record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
        record2.put("date", 1639145515643L)

        private val producer = getProducer
        producer.send(new ProducerRecord[String, GenericRecord](topic, id1, record1)).get
        producer.send(new ProducerRecord[String, GenericRecord](topic, id2, record2)).get

        private val kds = getStore
        private val fs = kds.getFeatureSource(topic)

        eventually(10, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 2
        }

        producer.send(new ProducerRecord[String, GenericRecord](topic, id1, null)).get

        eventually(10, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 1

          val feature = fs.getFeatures.features.next
          feature.getID mustEqual id2
          feature.getAttribute("shape") mustEqual expectedGeom2
          feature.getAttribute("date") mustEqual new Date(1639145515643L)
        }
      }

      "clear" in new ConfluentKafkaTestContext {
        private val id1 = "1"
        private val record1 = new GenericData.Record(schema2)
        private val expectedGeom1 = generatePolygon(Seq((0d,0d), (10d,0d), (10d,10d), (0d,10d), (0d,0d)))
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", 1639145281285L)

        private val id2 = "2"
        private val record2 = new GenericData.Record(schema2)
        private val expectedGeom2 = generatePolygon(Seq((5d,5d), (15d,5d), (15d,15d), (5d,15d), (5d,5d)))
        record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
        record2.put("date", 1639145515643L)

        private val producer = getProducer
        producer.send(new ProducerRecord[String, GenericRecord](topic, id1, record1)).get
        producer.send(new ProducerRecord[String, GenericRecord](topic, id2, record2)).get

        private val kds = getStore
        private val fs = kds.getFeatureSource(topic)

        eventually(10, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 2
        }

        producer.send(new ProducerRecord[String, GenericRecord](topic, "", null)).get

        eventually(10, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 0
        }
      }
    }
  }

  private trait ConfluentKafkaTestContext extends After {

    protected val confluentKafka: EmbeddedConfluent = new EmbeddedConfluent()

    private val geomFactory = new GeometryFactory()

    override def after: Unit = confluentKafka.close()

    protected def getProducer: KafkaProducer[String, GenericRecord] = {
      val producerProps = new Properties()
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, confluentKafka.brokers)
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
      producerProps.put("schema.registry.url", confluentKafka.schemaRegistryUrl)

      new KafkaProducer[String, GenericRecord](producerProps)
    }

    protected def getStore: KafkaDataStore = {
      val params = Map(
        "kafka.schema.registry.url" -> confluentKafka.schemaRegistryUrl,
        "kafka.brokers" -> confluentKafka.brokers,
        "kafka.zookeepers" -> confluentKafka.zookeepers,
        "kafka.topic.partitions" -> 1,
        "kafka.topic.replication" -> 1,
        "kafka.consumer.read-back" -> "Inf",
        "kafka.zk.path" -> "",
        "kafka.consumer.count" -> 1
      )

      DataStoreFinder.getDataStore(params).asInstanceOf[KafkaDataStore]
    }

    protected def generatePoint(x: Double, y: Double): Point = geomFactory.createPoint(new Coordinate(x, y))

    protected def generatePolygon(points: Seq[(Double, Double)]): Polygon = {
      geomFactory.createPolygon(points.map(point => new Coordinate(point._1, point._2)).toArray)
    }
  }
}
