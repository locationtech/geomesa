/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.nio.charset.StandardCharsets
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import java.{io, util}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Point
import kafka.admin.AdminUtils
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.metadata.CachedLazyMetadata
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.KafkaFeatureChanged
import org.locationtech.geomesa.kafka.{EmbeddedKafka, MockTicker}
import org.locationtech.geomesa.memory.cqengine.utils.{CQIndexType, CQIndexingOptions}
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends Specification with LazyLogging {

  sequential // this doesn't really need to be sequential, but we're trying to reduce zk load

  var kafka: EmbeddedKafka = _

  step {
    logger.info("Starting embedded kafka/zk")
    kafka = new EmbeddedKafka()
    logger.info("Started embedded kafka/zk")
  }

  lazy val baseParams = Map(
    "kafka.brokers"                 -> kafka.brokers,
    "kafka.zookeepers"              -> kafka.zookeepers,
    "kafka.topic.partitions"        -> 1,
    "kafka.topic.replication"       -> 1,
    "kafka.consumer.from-beginning" -> true
  )

  val gf = JTSFactoryFinder.getGeometryFactory
  val paths = new AtomicInteger(0)

  def newPath: String = s"geomesa/kafka/test/${paths.getAndIncrement()}"

  def getStore(zkPath: String, consumers: Int, extras: Map[String, AnyRef] = Map.empty): KafkaDataStore = {
    val params = baseParams ++ Map("kafka.zk.path" -> zkPath, "kafka.consumer.count" -> consumers) ++ extras
    DataStoreFinder.getDataStore(params).asInstanceOf[KafkaDataStore]
  }

  def createStorePair(params: Map[String, AnyRef] = Map.empty): (KafkaDataStore, KafkaDataStore, SimpleFeatureType) = {
    // note: the topic gets set in the user data, so don't re-use the same sft instance
    val sft = SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    CQIndexingOptions.setCQIndexType(sft.getDescriptor("name"), CQIndexType.UNIQUE)
    val path = newPath
    (getStore(path, 0, params), getStore(path, 1, params), sft)
  }

  "KafkaDataStore" should {

    "return correctly from canProcess" >> {
      import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams._
      val factory = new KafkaDataStoreFactory
      factory.canProcess(Map.empty[String, Serializable]) must beFalse
      factory.canProcess(Map(Brokers.key -> "test", Zookeepers.key -> "test")) must beTrue
    }

    "create unique topics based on zkPath" >> {
      val path = newPath
      val ds = getStore(path, 0)
      try {
        ds.createSchema(SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"))
        ds.getSchema("kafka").getUserData.get(KafkaDataStore.TopicKey) mustEqual s"$path-kafka".replaceAll("/", "-")
      } finally {
        ds.dispose()
      }
    }

    "use namespaces" >> {
      import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams._
      val path = newPath
      val ds = getStore(path, 0, Map(NamespaceParam.key -> "ns0"))
      try {
        ds.createSchema(SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"))
        ds.getSchema("kafka").getName.getNamespaceURI mustEqual "ns0"
        ds.getSchema("kafka").getName.getLocalPart mustEqual "kafka"
      } finally {
        ds.dispose()
      }
      val ds2 = getStore(path, 0, Map(NamespaceParam.key -> "ns1"))
      try {
        ds2.getSchema("kafka").getName.getNamespaceURI mustEqual "ns1"
        ds2.getSchema("kafka").getName.getLocalPart mustEqual "kafka"
      } finally {
        ds2.dispose()
      }
    }

    "allow schemas to be created and deleted" >> {
      foreach(Seq("true", "false")) { cqEngine =>
        CachedLazyMetadata.Expiry.threadLocalValue.set("10ms")
        val (producer, consumer, _) = try { createStorePair(Map("kafka.cache.cqengine" -> cqEngine)) } finally {
          CachedLazyMetadata.Expiry.threadLocalValue.remove()
        }
        consumer must not(beNull)
        producer must not(beNull)
        try {
          val sft = SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.foo='bar'")
          val topic = s"${producer.config.catalog}-${sft.getTypeName}".replaceAll("/", "-")
          producer.createSchema(sft)
          foreach(Seq(producer, consumer)) { ds =>
            ds.getTypeNames.toSeq mustEqual Seq(sft.getTypeName)
            val schema = ds.getSchema(sft.getTypeName)
            schema must not(beNull)
            schema mustEqual sft
            schema.getUserData.get("geomesa.foo") mustEqual "bar"
            schema.getUserData.get(KafkaDataStore.TopicKey) mustEqual topic
          }
          KafkaDataStore.withZk(kafka.zookeepers) { zk =>
            AdminUtils.topicExists(zk, topic) must beTrue
          }
          consumer.removeSchema(sft.getTypeName)
          foreach(Seq(consumer, producer)) { ds =>
            ds.getTypeNames.toSeq must eventually(40, 100.millis)(beEmpty)
            ds.getSchema(sft.getTypeName) must beNull
          }
          KafkaDataStore.withZk(kafka.zookeepers) { zk =>
            AdminUtils.topicExists(zk, topic) must eventually(40, 100.millis)(beFalse)
          }
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "write/update/read/delete features" >> {
      foreach(Seq("true", "false")) { cqEngine =>
        val (producer, consumer, sft) = createStorePair(Map("kafka.cache.cqengine" -> cqEngine))
        try {
          producer.createSchema(sft)
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach { f =>
              FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
              writer.write()
            }
          }
          SelfClosingIterator(store.getFeatures.features).toSeq must eventually(40, 100.millis)(containTheSameElementsAs(Seq(f0, f1)))

          // update
          f0.setAttributes(Array[AnyRef]("smith2", Int.box(32), "2017-01-01T00:00:02.000Z", "POINT (2 2)"))
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.copyToWriter(writer, f0, useProvidedFid = true)
            writer.write()
          }
          SelfClosingIterator(store.getFeatures.features).toSeq must eventually(40, 100.millis)(containTheSameElementsAs(Seq(f0, f1)))

          // query
          val queries = Seq(
            "strToUpperCase(name) = 'JONES'",
            "name = 'jones'",
            "age < 25",
            "bbox(geom, -15, -15, -5, -5) AND age < 25",
            "bbox(geom, -15, -15, 5, 5) AND dtg DURING 2017-01-01T12:00:00.000Z/2017-01-02T12:00:00.000Z",
            "INTERSECTS(geom, POLYGON((-11 -11, -9 -11, -9 -9, -11 -9, -11 -11))) AND bbox(geom, -15, -15, 5, 5)"
          )

          forall(queries) { ecql =>
            val query = new Query(sft.getTypeName, ECQL.toFilter(ecql))
            val features = SelfClosingIterator(consumer.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
            features mustEqual Seq(f1)
          }

          // delete
          producer.getFeatureSource(sft.getTypeName).removeFeatures(ECQL.toFilter("IN('sm')"))
          SelfClosingIterator(store.getFeatures.features).toSeq must eventually(40, 100.millis)(beEqualTo(Seq(f1)))

          // clear
          producer.getFeatureSource(sft.getTypeName).removeFeatures(Filter.INCLUDE)
          SelfClosingIterator(store.getFeatures.features).toSeq must eventually(40, 100.millis)(beEmpty)
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "write/read with visibilities" >> {
      skipped("intermittent failures")
      foreach(Seq("true", "false")) { cqEngine =>
        var auths: Set[String] = null
        val provider = new AuthorizationsProvider() {
          import scala.collection.JavaConversions._
          override def getAuthorizations: java.util.List[String] = auths.toList
          override def configure(params: util.Map[String, io.Serializable]): Unit = {}
        }

        val (producer, consumer, sft) = createStorePair(Map("kafka.cache.cqengine" -> cqEngine,
          org.locationtech.geomesa.security.AuthProviderParam.key -> provider))
        try {
          producer.createSchema(sft)
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          f0.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
          f1.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER&ADMIN")

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach { f =>
              FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
              writer.write()
            }
          }

          // admin user
          auths = Set("USER", "ADMIN")
          SelfClosingIterator(store.getFeatures.features).toSeq must eventually(40, 100.millis)(containTheSameElementsAs(Seq(f0, f1)))

          // regular user
          auths = Set("USER")
          SelfClosingIterator(store.getFeatures.features).toSeq mustEqual Seq(f0)

          // unauthorized
          auths = Set.empty
          SelfClosingIterator(store.getFeatures.features).toSeq must beEmpty
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "expire entries" >> {
      skipped("this test fails intermittently")
      val ticker = new MockTicker
      val expiry = Map("kafka.cache.expiry" -> "100ms", "kafka.cache.cleanup" -> "10ms", "kafka.cache.ticker" -> ticker)
      foreach(Seq("true", "false")) { cqEngine =>
        val (producer, consumer, sft) = createStorePair(Map("kafka.cache.cqengine" -> cqEngine) ++ expiry)
        try {
          producer.createSchema(sft)
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")

          val bbox = ECQL.toFilter("bbox(geom,-10,-10,10,10)")

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach { f =>
              FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
              writer.write()
            }
          }
          // check the cache directly
          SelfClosingIterator(store.getFeatures.features).toSeq must
              eventually(40, 100.millis)(containTheSameElementsAs(Seq(f0, f1)))
          // check the spatial index
          SelfClosingIterator(store.getFeatures(bbox).features).toSeq must
              eventually(40, 100.millis)(containTheSameElementsAs(Seq(f0, f1)))

          // allow the cache to expire
          ticker.millis += 1000

          // verify feature has expired - hit the cache directly
          SelfClosingIterator(store.getFeatures.features) must eventually(40, 100.millis)(beEmpty)
          // verify feature has expired - hit the spatial index
          SelfClosingIterator(store.getFeatures(bbox).features) must eventually(40, 100.millis)(beEmpty)
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "support listeners" >> {
      val (producer, consumer, sft) = createStorePair()
      try {
        val id = "fid-0"
        val numUpdates = 100
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        count must eventually(40, 100.millis)(beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "migrate old kafka data store schemas" >> {
      val spec = "test:String,dtg:Date,*geom:Point:srid=4326"

      val path = newPath
      val client = CuratorFrameworkFactory.builder()
          .namespace(path)
          .connectString(kafka.zookeepers)
          .retryPolicy(new ExponentialBackoffRetry(1000, 3))
          .build()
      client.start()

      try {
        client.create.forPath("/test", s"$spec;geomesa.index.dtg=dtg".getBytes(StandardCharsets.UTF_8))
        client.create.forPath("/test/Topic", "test-topic".getBytes(StandardCharsets.UTF_8))

        val ds = getStore(path, 0)
        try {
          ds.getTypeNames.toSeq mustEqual Seq("test")
          val sft = ds.getSchema("test")
          sft must not(beNull)
          KafkaDataStore.topic(sft) mustEqual "test-topic"
          SimpleFeatureTypes.encodeType(sft) mustEqual spec

          client.checkExists().forPath("/test") must beNull
        } finally {
          ds.dispose()
        }
      } finally {
        client.close()
      }
    }
  }

  "KafkaDataStoreFactory" should {
    "clean zkPath" >> {
      def getNamespace(path: String): String =
        KafkaDataStoreFactory.createZkNamespace(Map(KafkaDataStoreFactoryParams.ZkPath.getName -> path))

      // a well formed path starts does not start or end with a /
      getNamespace("foo/bar/baz") mustEqual "foo/bar/baz"
      getNamespace("foo/bar/baz/") mustEqual "foo/bar/baz" // trailing slash
      getNamespace("/foo/bar/baz") mustEqual "foo/bar/baz" // leading slash
      getNamespace("/foo/bar/baz/") mustEqual "foo/bar/baz" // both leading and trailing slash
      forall(Seq("/", "//", "", null))(n => getNamespace(n) mustEqual KafkaDataStoreFactory.DefaultZkPath) // empty
    }
  }

  step {
    logger.info("Stopping embedded kafka/zk")
    kafka.close()
    logger.info("Stopped embedded kafka/zk")
  }
}
