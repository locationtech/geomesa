/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.jts.geom.Point
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConfluentKafkaDataStoreTest extends Specification with Mockito with LazyLogging {

  import scala.collection.JavaConversions._
  import scala.concurrent.duration._

  sequential // this doesn't really need to be sequential, but we're trying to reduce zk load

  var kafka: EmbeddedConfluent = _

  step {
    logger.info("Starting embedded kafka/zk")
    kafka = new EmbeddedConfluent()
    logger.info("Started embedded kafka/zk")
  }

  lazy val baseParams = Map(
    "kafka.schema.registry.url" -> kafka.schemaRegistryUrl,
    "kafka.brokers"             -> kafka.brokers,
    "kafka.zookeepers"          -> kafka.zookeepers,
    "kafka.topic.partitions"    -> 1,
    "kafka.topic.replication"   -> 1,
    "kafka.consumer.read-back"  -> "Inf"
  )

  def getStore(zkPath: String, consumers: Int, extras: Map[String, AnyRef] = Map.empty): KafkaDataStore = {
    val params = baseParams ++ Map("kafka.zk.path" -> zkPath, "kafka.consumer.count" -> consumers) ++ extras
    DataStoreFinder.getDataStore(params).asInstanceOf[KafkaDataStore]
  }

  "ConfluentKafkaDataStore" should {
    "read schemas from confluent schema registry" >> {
      val topic = "confluent-test"
      val producerProps = new Properties()
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokers)
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
      producerProps.put("schema.registry.url", kafka.schemaRegistryUrl)
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

      val kds = getStore(zkPath = "", consumers = 1)
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

  step {
    logger.info("Stopping embedded kafka/zk")
    kafka.close()
    logger.info("Stopped embedded kafka/zk")
  }
}
