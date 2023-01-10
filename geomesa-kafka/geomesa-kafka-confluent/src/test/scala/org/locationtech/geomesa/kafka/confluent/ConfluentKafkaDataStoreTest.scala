/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.geotools.api.data.{DataStoreFinder, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.confluent.ConfluentKafkaDataStoreTest._
import org.locationtech.geomesa.kafka.confluent.SchemaParser._
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom._
import org.specs2.runner.JUnitRunner

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Date, Properties}
import scala.collection.JavaConverters._
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ConfluentKafkaDataStoreTest extends ConfluentContainerTest {

  private val geomFactory = new GeometryFactory()
  private val topicCounter = new AtomicInteger(0)

  lazy val producer: KafkaProducer[String, GenericRecord] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put("schema.registry.url", schemaRegistryUrl)
    new KafkaProducer[String, GenericRecord](props)
  }

  lazy val kds = getStore()

  def getStore(extraParams: Map[String, String] = Map.empty): KafkaDataStore = {
    val params = Map(
      "kafka.schema.registry.url" -> schemaRegistryUrl,
      "kafka.brokers" -> brokers,
      "kafka.zookeepers" -> zookeepers,
      "kafka.topic.partitions" -> 1,
      "kafka.topic.replication" -> 1,
      "kafka.consumer.read-back" -> "Inf",
      "kafka.zk.path" -> "",
      "kafka.consumer.count" -> 1
    ) ++ extraParams

    DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KafkaDataStore]
  }

  def generatePoint(x: Double, y: Double): Point = geomFactory.createPoint(new Coordinate(x, y))

  def generatePolygon(points: Seq[(Double, Double)]): Polygon =
    geomFactory.createPolygon(points.map(point => new Coordinate(point._1, point._2)).toArray)

  def newTopic(): String = s"confluent-kds-${topicCounter.getAndIncrement()}-test"

  "ConfluentKafkaDataStore" should {
    "fail to get a feature source when the schema cannot be converted to an SFT" in {
      val topic = newTopic()
      val record = new GenericData.Record(badSchema)
      record.put("f1", "POINT(10 20)")

      producer.send(new ProducerRecord[String, GenericRecord](topic, "1", record)).get

      // the geometry field cannot be parsed so the SFT never gets created and CKDS can't find the schema
      kds.getFeatureSource(topic) must throwAn[Exception]
    }

    "deserialize simple features when the schema and records are valid" in {
      val topic = newTopic()
      val id1 = "1"
      val record1 = new GenericData.Record(schema1)
      record1.put("id", id1)
      record1.put("position", "POINT(10 20)")
      record1.put("speed", 12.325d)
      record1.put("date", "2021-12-07T17:22:24.897-05:00")
      record1.put("visibility", "")

      val id2 = "2"
      val record2 = new GenericData.Record(schema1)
      record2.put("id", id1)
      record2.put("position", "POINT(15 35)")
      record2.put("speed", 0.00d)
      record2.put("date", "2021-12-07T17:23:25.372-05:00")
      record2.put("visibility", "hidden")

      producer.send(new ProducerRecord[String, GenericRecord](topic, id1, record1)).get
      producer.send(new ProducerRecord[String, GenericRecord](topic, id2, record2)).get

      val fs = kds.getFeatureSource(topic)

      eventually(20, 100.millis) {
        // the record with "hidden" visibility doesn't appear because no auths are configured
        val features = SelfClosingIterator(fs.getFeatures.features).toList
        features must haveLength(1)
        val feature = features.head
        val expectedPosition = generatePoint(10d, 20d)
        SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft1
        feature.getID mustEqual id1
        feature.getAttribute("position") mustEqual expectedPosition
        feature.getAttribute("speed") mustEqual 12.325d
        feature.getAttribute("date") mustEqual new Date(1638915744897L)
        feature.getDefaultGeometry mustEqual expectedPosition
        SecurityUtils.getVisibility(feature) mustEqual ""
      }
    }

    "produce messages based on simple features" in {
      val topic = newTopic()
      new CachedSchemaRegistryClient(schemaRegistryUrl, 100).register(s"$topic-value", new AvroSchema(schema1))

      val sft = kds.getSchema(topic)
      sft must not(beNull)

      val id1 = "1"
      val sf1 = ScalaSimpleFeature.create(sft, id1, id1, "POINT (10 20)", 12.325d, "2021-12-07T22:22:24.897Z")
      val id2 = "2"
      val sf2 = ScalaSimpleFeature.create(sft, id2, id2, "POINT (15 35)", 0, "2021-12-07T22:23:25.372Z")
      Seq(sf1, sf2).foreach(SecurityUtils.setFeatureVisibility(_, ""))

      WithClose(kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT)) { writer =>
        Seq(sf1, sf2).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val fs = kds.getFeatureSource(topic)

      eventually(20, 100.millis) {
        val features = SelfClosingIterator(fs.getFeatures.features).toList
        features must haveLength(2)
        features.map(_.getID) must containTheSameElementsAs(Seq(id1, id2))
        foreach(features)(f => SimpleFeatureTypes.encodeType(f.getType) mustEqual encodedSft1)

        val feature1 = features.find(_.getID == id1).get
        feature1.getAttribute("position") must beAnInstanceOf[Geometry]
        WKTUtils.write(feature1.getAttribute("position").asInstanceOf[Geometry]) mustEqual "POINT (10 20)"
        feature1.getAttribute("speed") mustEqual 12.325d
        feature1.getAttribute("date") mustEqual new Date(1638915744897L)

        val feature2 = features.find(_.getID == id2).get
        feature2.getAttribute("position") must beAnInstanceOf[Geometry]
        WKTUtils.write(feature2.getAttribute("position").asInstanceOf[Geometry]) mustEqual "POINT (15 35)"
        feature2.getAttribute("speed") mustEqual 0d
        feature2.getAttribute("date") mustEqual new Date(1638915805372L)

        foreach(features)(f => SecurityUtils.getVisibility(f) mustEqual "")
      }

      // verify raw avro reads
      val consumer = {
        val props = new Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
        props.put("schema.registry.url", schemaRegistryUrl)
        new KafkaConsumer[String, GenericRecord](props)
      }
      try {
        consumer.subscribe(Collections.singletonList(topic))

        val records = scala.collection.mutable.Set.empty[(String, GenericRecord)]
        eventually(20, 100.millis) {
          consumer.poll(java.time.Duration.ofMillis(10)).asScala.foreach(r => records += r.key() -> r.value())
          records must haveLength(2)
        }
        records.map(_._1) mustEqual Set(id1, id2)

        val avro = records.toSeq.sortBy(_._1).map(_._2)
        avro.map(_.get("id")) mustEqual Seq(id1, id2).map(new Utf8(_))
        avro.map(_.get("date")) mustEqual Seq("2021-12-07T22:22:24.897Z", "2021-12-07T22:23:25.372Z").map(new Utf8(_))
        avro.map(_.get("speed")) mustEqual Seq(12.325d, 0d)
        avro.map(_.get("position")) mustEqual Seq("POINT (10 20)", "POINT (15 35)").map(new Utf8(_))
        avro.map(_.get("visibility")) mustEqual Seq("", "").map(new Utf8(_))
      } finally {
        consumer.close()
      }
    }

    "use the schema overrides config if available" in {
      val topic = newTopic()
      val id = "1"
      val record = new GenericData.Record(schema2_NoGeoMesa)
      val expectedGeom = generatePoint(-12.2d, 120.7d)
      record.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom)))
      record.put("date", 1639145281285L)

      producer.send(new ProducerRecord[String, GenericRecord](topic, id, record)).get

      val schemaOverridesConfig =
        ConfluentKafkaDataStoreFactoryTest.generateSchemaOverrideConfig(Map(topic -> schemaJson2))
      val extraParams = Map("kafka.schema.overrides" -> schemaOverridesConfig)
      WithClose(getStore(extraParams)) { kds =>
        val fs = kds.getFeatureSource(topic)

        eventually(20, 100.millis) {
          val features = SelfClosingIterator(fs.getFeatures.features).toList
          features must haveLength(1)
          val feature = features.head
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
          feature.getID mustEqual id
          feature.getAttribute("shape") mustEqual expectedGeom
          feature.getAttribute("date") mustEqual new Date(1639145281285L)
        }
      }
    }

    "support logical date types" in {
      val topic = newTopic()
      val id = "1"
      val record = new GenericData.Record(logicalDateTypeSchema)
      val expectedGeom = generatePoint(-12.2d, 120.7d)
      record.put("geom", WKTUtils.write(expectedGeom))
      record.put("dtg", 1639145281285L)

      producer.send(new ProducerRecord[String, GenericRecord](topic, id, record)).get

      WithClose(getStore()) { kds =>
        val fs = kds.getFeatureSource(topic)

        eventually(20, 100.millis) {
          val features = SelfClosingIterator(fs.getFeatures.features).toList
          features must haveLength(1)
          val feature = features.head
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSftLogicalDate
          feature.getID mustEqual id
          feature.getAttribute("geom") mustEqual expectedGeom
          feature.getAttribute("dtg") mustEqual new Date(1639145281285L)
        }
      }
    }

    "support the GeoMessage control operation for update" in {
      val topic = newTopic()
      val id = "1"
      val record1 = new GenericData.Record(schema2)
      val expectedGeom1 = generatePoint(10d, 20d)
      record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
      record1.put("date", 1639145281285L)

      producer.send(new ProducerRecord[String, GenericRecord](topic, id, record1)).get

      val fs = kds.getFeatureSource(topic)

      eventually(20, 100.millis) {
        val features = SelfClosingIterator(fs.getFeatures.features).toList
        features must haveLength(1)
        val feature = features.head
        SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
        feature.getID mustEqual id
        feature.getAttribute("shape") mustEqual expectedGeom1
        feature.getAttribute("date") mustEqual new Date(1639145281285L)
      }

      val record2 = new GenericData.Record(schema2)
      val expectedGeom2 = generatePoint(15d, 30d)
      record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
      record2.put("date", null)

      producer.send(new ProducerRecord[String, GenericRecord](topic, id, record2)).get

      eventually(20, 100.millis) {
        val features = SelfClosingIterator(fs.getFeatures.features).toList
        features must haveLength(1)
        val feature = features.head
        SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
        feature.getID mustEqual id
        feature.getAttribute("shape") mustEqual expectedGeom2
        feature.getAttribute("date") mustEqual null
      }
    }

    "support the GeoMessage control operation for drop" in {
      val topic = newTopic()
      val id1 = "1"
      val record1 = new GenericData.Record(schema2)
      val expectedGeom1 = generatePolygon(Seq((0d,0d), (10d,0d), (10d,10d), (0d,10d), (0d,0d)))
      record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
      record1.put("date", 1639145281285L)

      val id2 = "2"
      val record2 = new GenericData.Record(schema2)
      val expectedGeom2 = generatePolygon(Seq((5d,5d), (15d,5d), (15d,15d), (5d,15d), (5d,5d)))
      record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
      record2.put("date", 1639145515643L)

      producer.send(new ProducerRecord[String, GenericRecord](topic, id1, record1)).get
      producer.send(new ProducerRecord[String, GenericRecord](topic, id2, record2)).get

      val fs = kds.getFeatureSource(topic)

      eventually(20, 100.millis) {
        SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 2
      }

      producer.send(new ProducerRecord[String, GenericRecord](topic, id1, null)).get

      eventually(20, 100.millis) {
        val features = SelfClosingIterator(fs.getFeatures.features).toList
        features must haveLength(1)
        val feature = features.head
        SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
        feature.getID mustEqual id2
        feature.getAttribute("shape") mustEqual expectedGeom2
        feature.getAttribute("date") mustEqual new Date(1639145515643L)
      }
    }

    "support the GeoMessage control operation for clear" in {
      val topic = newTopic()
      val id1 = "1"
      val record1 = new GenericData.Record(schema2)
      val expectedGeom1 = generatePolygon(Seq((0d,0d), (10d,0d), (10d,10d), (0d,10d), (0d,0d)))
      record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
      record1.put("date", 1639145281285L)

      val id2 = "2"
      val record2 = new GenericData.Record(schema2)
      val expectedGeom2 = generatePolygon(Seq((5d,5d), (15d,5d), (15d,15d), (5d,15d), (5d,5d)))
      record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
      record2.put("date", 1639145515643L)

      producer.send(new ProducerRecord[String, GenericRecord](topic, id1, record1)).get
      producer.send(new ProducerRecord[String, GenericRecord](topic, id2, record2)).get

      val fs = kds.getFeatureSource(topic)

      eventually(20, 100.millis) {
        SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 2
      }

      producer.send(new ProducerRecord[String, GenericRecord](topic, "", null)).get

      eventually(20, 100.millis) {
        SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 0
      }
    }
  }

  step {
    producer.close()
    kds.dispose()
  }
}

object ConfluentKafkaDataStoreTest {

  val schemaJson1: String =
    s"""{
       |  "type":"record",
       |  "name":"schema1",
       |  "geomesa.index.dtg":"date",
       |  "fields":[
       |    {
       |      "name":"id",
       |      "type":"string",
       |      "cardinality":"high"
       |    },
       |    {
       |      "name":"position",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKT}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POINT}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"speed",
       |      "type":"double"
       |    },
       |    {
       |      "name":"date",
       |      "type":"string",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.ISO_DATETIME}"
       |    },
       |    {
       |      "name":"visibility",
       |      "type":"string",
       |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}",
       |      "${GeoMesaAvroExcludeField.KEY}":"${GeoMesaAvroExcludeField.TRUE}"
       |    }
       |  ]
       |}""".stripMargin
  val schema1: Schema = new Schema.Parser().parse(schemaJson1)
<<<<<<< HEAD
  val encodedSft1: String = s"id:String:cardinality=high,*position:Point:srid=4326,speed:Double,date:Date:geomesa.date.format='iso-datetime'"
=======
  val encodedSft1: String = s"id:String:cardinality=high,*position:Point:srid=4326,speed:Double,date:Date"
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

  val schemaJson2: String =
    s"""{
       |  "type":"record",
       |  "name":"schema2",
       |  "fields":[
       |    {
       |      "name":"shape",
       |      "type":"bytes",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.GEOMETRY}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"date",
       |      "type":["null","long"],
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.EPOCH_MILLIS}"
       |    }
       |  ]
       |}""".stripMargin
  val schema2: Schema = new Schema.Parser().parse(schemaJson2)
<<<<<<< HEAD
  val encodedSft2: String = "*shape:Geometry:srid=4326,date:Date:geomesa.date.format='epoch-millis'"
=======
  val encodedSft2: String = s"*shape:Geometry:srid=4326,date:Date"
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

  val schemaJson2_NoGeoMesa: String =
    s"""{
       |  "type":"record",
       |  "name":"schema2",
       |  "fields":[
       |    {
       |      "name":"shape",
       |      "type":"bytes"
       |    },
       |    {
       |      "name":"date",
       |      "type":["null","long"]
       |    }
       |  ]
       |}""".stripMargin
  val schema2_NoGeoMesa: Schema = new Schema.Parser().parse(schemaJson2_NoGeoMesa)

  val badSchemaJson: String =
    s"""{
       |  "type":"record",
       |  "name":"schema",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"bad-format",
       |      "${GeoMesaAvroGeomType.KEY}":"bad-type"
       |    }
       |  ]
       |}""".stripMargin
  val badSchema: Schema = new Schema.Parser().parse(badSchemaJson)

  val logicalDateTypeSchemaJson: String =
    s"""{
       |  "type":"record",
       |  "name":"schema3",
       |  "fields":[
       |    {
       |      "name":"geom",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"wkt",
       |      "${GeoMesaAvroGeomType.KEY}":"point"
       |    },
       |    {
       |      "name":"dtg",
       |      "type": { "type": "long", "logicalType": "timestamp-millis" }
       |    }
       |  ]
       |}""".stripMargin
  val logicalDateTypeSchema: Schema = new Schema.Parser().parse(logicalDateTypeSchemaJson)
  val encodedSftLogicalDate: String = "*geom:Point:srid=4326,dtg:Date"
}
