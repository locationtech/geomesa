/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka21

import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import kafka.admin.AdminUtils
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.TestLambdaFeatureListener
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Try


@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends Specification with LazyLogging {

  import org.locationtech.geomesa.security._

  import scala.collection.JavaConversions._

  sequential // this doesn't really need to be sequential, but we're trying to reduce zk load

  var kafka: EmbeddedKafka = _

  step {
    logger.info("Starting embedded kafka/zk")
    kafka = new EmbeddedKafka()
    logger.info("Started embedded kafka/zk")
  }

  val unique = new AtomicInteger(0)

  def newSchema: SimpleFeatureType = {
    val name = s"kafka-test-${unique.getAndIncrement()}"
    SimpleFeatureTypes.createType(name, s"name:String,age:Int,dtg:Date,*geom:Point:srid=4326;Topic=$name")
  }

  lazy val pair: Seq[Map[String, String]] = {
    val base = Map(
      "brokers"         -> kafka.brokers,
      "zookeepers"      -> kafka.zookeepers,
      "zkPath"          -> "/geomesa/kafka/testds",
      "autoOffsetReset" -> "earliest"
    )
    Seq(base + ("isProducer" -> "true"), base + ("isProducer" -> "false"))
  }

  "KafkaDataStore" should {

    "return correctly from canProcess" >> {
      val factory = new KafkaDataStoreFactory
      factory.canProcess(Map.empty[String, Serializable]) must beFalse
      factory.canProcess(
        Map(
          KafkaDataStoreFactoryParams.KAFKA_BROKER_PARAM.key -> "test",
          KafkaDataStoreFactoryParams.ZOOKEEPERS_PARAM.key -> "test"
        )
      ) must beTrue
    }

    "get a data store" in {
      foreach(pair) { params =>
        val ds = DataStoreFinder.getDataStore(params)
        ds must not(beNull)
        ds.dispose()
        ok
      }
    }

    "fail to create a schema if topic name is not supplied" in {
      val sft = newSchema
      sft.getUserData.remove("Topic")
      foreach(pair) { params =>
        val ds = DataStoreFinder.getDataStore(params)
        try {
          ds.createSchema(sft) must throwAn[IllegalArgumentException]
        } finally {
          ds.dispose()
        }
      }
    }

    "create and delete schemas and topics" in {
      val sft = newSchema
      withStorePair(pair) { case (producerStore, consumerStore) =>
        producerStore.createSchema(sft)
        KafkaDataStore.withZk(kafka.zookeepers)(AdminUtils.topicExists(_, sft.getTypeName)) must beTrue
        foreach(Seq(consumerStore, producerStore))(_.getSchema(sft.getTypeName) mustEqual sft)
        foreach(Seq(consumerStore, producerStore))(_.createSchema(sft) must throwAn[IllegalArgumentException])
        consumerStore.removeSchema(sft.getTypeName)
        // get a new store - the old ones will have cached the sft
        withStorePair(pair) { case (ds1, ds2) =>
          foreach(Seq(ds1, ds2))(_.getSchema(sft.getTypeName) must throwAn[IOException])
        }
        // TODO topic deletion is not enabled?
        // KafkaDataStore.withZk(kafka.zookeepers)(AdminUtils.topicExists(_, sft.getTypeName)) must beFalse
      }
    }

    "return the correct type names" in {
      val sfts = Seq.tabulate(3)(i => (newSchema, i))
      val ds = DataStoreFinder.getDataStore(pair.last + ("zkPath" -> "/geomesa/kafka/typenames"))
      try {
        ds.getTypeNames must beEmpty
        foreach(sfts) { case (sft, i) =>
          ds.createSchema(sft)
          ds.getTypeNames.toSeq must containTheSameElementsAs(sfts.take(i + 1).map(_._1.getTypeName))
        }
      } finally {
        ds.dispose()
      }
    }

    "read and write features" in {
      val sft = newSchema
      val sf = ScalaSimpleFeature.create(sft, "0", "smith", 30, "2018-01-01T00:00:00.000Z", "POINT (0 0)")
      sf.visibility = "USER|ADMIN"
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

      withStorePair(pair) { case (producerStore, consumerStore) =>
        producerStore.createSchema(sft)

        val consumer = consumerStore.getFeatureSource(sft.getTypeName)
        val producer = producerStore.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]

        producer.addFeatures(new ListFeatureCollection(sft, Array[SimpleFeature](sf)))
        consumer.getFeatures.features().hasNext must eventually(beTrue)
        SelfClosingIterator(consumer.getFeatures().features).toList mustEqual Seq(sf)
        consumer.getFeatures.size() mustEqual 1

        sf.setAttribute("name", "jones")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        producer.addFeatures(new ListFeatureCollection(sft, Array[SimpleFeature](sf)))
        SelfClosingIterator(consumer.getFeatures().features).toList must eventually(beEqualTo(Seq(sf)))
        consumer.getFeatures.size() mustEqual 1

        producer.removeFeatures(ECQL.toFilter("IN ('0')"))
        SelfClosingIterator(consumer.getFeatures().features).toList must eventually(beEmpty[List[SimpleFeature]])
        consumer.getFeatures.size() mustEqual 0
      }
    }

    "query with cql" >> {
      val sft = newSchema
      val sf1 = ScalaSimpleFeature.create(sft, "0", "smith", 30, "2018-01-01T00:00:00.000Z", "POINT (0 0)")
      val sf2 = ScalaSimpleFeature.create(sft, "1", "jones", 40, "2018-01-02T00:00:00.000Z", "POINT (10 10)")
      Seq(sf1, sf2).foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))

      withStorePair(pair) { case (producerStore, consumerStore) =>
        producerStore.createSchema(sft)

        val consumer = consumerStore.getFeatureSource(sft.getTypeName)
        val producer = producerStore.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]

        producer.addFeatures(new ListFeatureCollection(sft, Array[SimpleFeature](sf1, sf2)))
        SelfClosingIterator(consumer.getFeatures().features).toList must eventually(containTheSameElementsAs(Seq[SimpleFeature](sf1, sf2)))

        SelfClosingIterator(consumer.getFeatures(ECQL.toFilter("age > 35")).features).toList mustEqual Seq(sf2)
        SelfClosingIterator(consumer.getFeatures(ECQL.toFilter("bbox(geom, -5, -5, 5, 5)")).features).toList mustEqual Seq(sf1)
        SelfClosingIterator(consumer.getFeatures(ECQL.toFilter("name in ('smith') AND bbox(geom, -5, -5, 5, 5)")).features).toList mustEqual Seq(sf1)
      }
    }

    "allow for configurable expiration" >> {
      val sft = newSchema
      val sf = ScalaSimpleFeature.create(sft, "0", "smith", 30, "2018-01-01T00:00:00.000Z", "POINT (0 0)")
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

      val params = pair.map(_ + (KafkaDataStoreFactoryParams.EXPIRATION_PERIOD.getName -> "1000"))
      withStorePair(params) { case (producerStore, consumerStore) =>
        producerStore.createSchema(sft)

        val consumer = consumerStore.getFeatureSource(sft.getTypeName)

        val fw = producerStore.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        FeatureUtils.copyToWriter(fw, sf)
        fw.write()
        fw.close()

        val bbox = ECQL.toFilter("bbox(geom,-10,-10,10,10)")

        // verify the feature is written - check the direct cache and the spatial index
        SelfClosingIterator(consumer.getFeatures().features).toList must eventually(beEqualTo(Seq(sf)))
        SelfClosingIterator(consumer.getFeatures(bbox).features).toList must eventually(beEqualTo(Seq(sf)))

        Thread.sleep(1000)

        // force the cache cleanup - normally this would happen during additional reads and writes
        consumer.asInstanceOf[KafkaConsumerFeatureSource].featureCache.cleanUp()

        // verify expiration - check the direct cache and the spatial index
        SelfClosingIterator(consumer.getFeatures().features).toList must eventually(beEmpty[List[SimpleFeature]])
        SelfClosingIterator(consumer.getFeatures(bbox).features).toList must eventually(beEmpty[List[SimpleFeature]])
      }
    }

    "support listeners" >> {
      val sft = newSchema

      withStorePair(pair) { case (producerStore, consumerStore) =>
        producerStore.createSchema(sft)

        val consumer = consumerStore.getFeatureSource(sft.getTypeName)

        val numUpdates = 100
        val maxLon = 80.0

        val latch = new CountDownLatch(numUpdates)

        consumer.addFeatureListener(new TestLambdaFeatureListener(_ => latch.countDown()))

        val fw = producerStore.getFeatureWriter(sft.getTypeName, null, Transaction.AUTO_COMMIT)

        val sf = ScalaSimpleFeature.create(sft, "0", "smith", 30, "2018-01-01T00:00:00.000Z", "POINT (0 0)")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

        var i = numUpdates
        while (i > 0) {
          val ll = maxLon - maxLon / i
          sf.setAttribute("geom", s"POINT ($ll $ll)")
          FeatureUtils.copyToWriter(fw, sf)
          fw.write()
          i -= 1
        }

        latch.getCount must eventually(beEqualTo(0L))
        SelfClosingIterator(consumer.getFeatures().features).toList must eventually(beEqualTo(Seq(sf)))
      }
    }
  }

  def withStorePair[T](params: Seq[Map[String, String]])(fn: (DataStore, DataStore) => T): T = {
    val Seq(producer, consumer) = params.map(DataStoreFinder.getDataStore(_))
    try {
      fn(producer, consumer)
    } finally {
      Try(producer.dispose())
      Try(consumer.dispose())
    }
  }

  step {
    logger.info("Stopping embedded kafka/zk")
    kafka.close()
    logger.info("Stopped embedded kafka/zk")
  }
}
