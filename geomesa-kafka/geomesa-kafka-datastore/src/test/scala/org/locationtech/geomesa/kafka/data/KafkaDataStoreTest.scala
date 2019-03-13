/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.{Collections, Date}

import com.typesafe.scalalogging.LazyLogging
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
import org.locationtech.geomesa.kafka.EmbeddedKafka
import org.locationtech.geomesa.kafka.ExpirationMocking.{ScheduledExpiry, WrappedRunnable}
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.KafkaFeatureChanged
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils}
import org.locationtech.geomesa.utils.cache.Ticker
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Point
import org.mockito.ArgumentMatchers
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends Specification with Mockito with LazyLogging {

  import scala.collection.JavaConversions._
  import scala.concurrent.duration._

  sequential // this doesn't really need to be sequential, but we're trying to reduce zk load

  var kafka: EmbeddedKafka = _

  step {
    logger.info("Starting embedded kafka/zk")
    kafka = new EmbeddedKafka()
    logger.info("Started embedded kafka/zk")
  }

  lazy val baseParams = Map(
//    "kafka.serialization.type" -> "avro",
    "kafka.brokers"            -> kafka.brokers,
    "kafka.zookeepers"         -> kafka.zookeepers,
    "kafka.topic.partitions"   -> 1,
    "kafka.topic.replication"  -> 1,
    "kafka.consumer.read-back" -> "Inf"
  )

  val gf = JTSFactoryFinder.getGeometryFactory
  val paths = new AtomicInteger(0)

  def getStore(zkPath: String, consumers: Int, extras: Map[String, AnyRef] = Map.empty): KafkaDataStore = {
    val params = baseParams ++ Map("kafka.zk.path" -> zkPath, "kafka.consumer.count" -> consumers) ++ extras
    DataStoreFinder.getDataStore(params).asInstanceOf[KafkaDataStore]
  }

  def createStorePair(name: String,
                      params: Map[String, AnyRef] = Map.empty): (KafkaDataStore, KafkaDataStore, SimpleFeatureType) = {
    // note: the topic gets set in the user data, so don't re-use the same sft instance
    val sft = SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val path = s"geomesa/$name/test/${paths.getAndIncrement()}"
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
      val path = s"geomesa/topics/test/${paths.getAndIncrement()}"
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
      val path = s"geomesa/namespace/test/${paths.getAndIncrement()}"
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
      foreach(Seq(true, false)) { cqEngine =>
        CachedLazyMetadata.Expiry.threadLocalValue.set("10ms")
        val (producer, consumer, _) = try {
          val params = if (cqEngine) {
            Map("kafka.index.cqengine" -> "geom:default,name:unique")
          } else {
            Map.empty[String, String]
          }
          createStorePair("createdelete", params)
        } finally {
          CachedLazyMetadata.Expiry.threadLocalValue.remove()
        }
        consumer must not(beNull)
        producer must not(beNull)
        try {
          val sft = SimpleFeatureTypes.createImmutableType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.foo='bar'")
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
            eventually(40, 100.millis)(ds.getTypeNames.toSeq must beEmpty)
            ds.getSchema(sft.getTypeName) must beNull
          }
          KafkaDataStore.withZk(kafka.zookeepers) { zk =>
            eventually(40, 100.millis)(AdminUtils.topicExists(zk, topic) must beFalse)
          }
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "write/update/read/delete features" >> {
      foreach(Seq(true, false)) { cqEngine =>
        val params = if (cqEngine) {
          Map("kafka.index.cqengine" -> "geom:default,name:unique")
        } else {
          Map.empty[String, String]
        }
        val (producer, consumer, sft) = createStorePair("writeupdatedelete", params)
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
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must containTheSameElementsAs(Seq(f0, f1)))

          // update
          val f2 = ScalaSimpleFeature.create(sft, "sm", "smith2", 32, "2017-01-01T00:00:02.000Z", "POINT (2 2)")
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.copyToWriter(writer, f2, useProvidedFid = true)
            writer.write()
          }
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must containTheSameElementsAs(Seq(f1, f2)))

          // query
          val queries = Seq(
            "strToUpperCase(name) = 'JONES'",
            "name = 'jones' OR name = 'smith'",
            "name = 'foo' OR name = 'bar' OR name = 'baz' OR name = 'blarg' OR name = 'jones' OR name = 'smith'",
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
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must beEqualTo(Seq(f1)))

          // clear
          producer.getFeatureSource(sft.getTypeName).removeFeatures(Filter.INCLUDE)
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must beEmpty)
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "write/read with visibilities" >> {
      import org.locationtech.geomesa.security.AuthProviderParam

      foreach(Seq(true, false)) { cqEngine =>
        var auths: Set[String] = null
        val provider = new AuthorizationsProvider() {
          import scala.collection.JavaConversions._
          override def getAuthorizations: java.util.List[String] = auths.toList
          override def configure(params: java.util.Map[String, java.io.Serializable]): Unit = {}
        }
        val params = if (cqEngine) {
          Map("kafka.index.cqengine" -> "geom:default,name:unique")
        } else {
          Map.empty[String, String]
        }
        val (producer, consumer, sft) = createStorePair("vis", params + (AuthProviderParam.key -> provider))
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
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must containTheSameElementsAs(Seq(f0, f1)))

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
      foreach(Seq(true, false)) { cqEngine =>
        val executor = mock[ScheduledExecutorService]
        val ticker = Ticker.mock(System.currentTimeMillis())
        val params = if (cqEngine) {
          Map("kafka.cache.expiry" -> "100ms",
            "kafka.cache.executor" -> (executor, ticker),
            "kafka.index.cqengine" -> "geom:default,name:unique")
        } else {
          Map("kafka.cache.expiry" -> "100ms", "kafka.cache.executor" -> (executor, ticker))
        }
        val (producer, consumer, sft) = createStorePair("expire", params)
        try {
          producer.createSchema(sft)
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")

          val bbox = ECQL.toFilter("bbox(geom,-10,-10,10,10)")

          val expirations = Collections.synchronizedList(new java.util.ArrayList[WrappedRunnable](2))
          executor.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.anyLong(), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
            val expire = new WrappedRunnable(0L)
            expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
            expirations.add(expire)
            new ScheduledExpiry(expire)
          }

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach { f =>
              FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
              writer.write()
            }
          }
          // check the cache directly
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must
              containTheSameElementsAs(Seq(f0, f1)))
          // check the spatial index
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures(bbox).features).toSeq must
              containTheSameElementsAs(Seq(f0, f1)))

          // expire the cache
          expirations.foreach(_.runnable.run())

          // verify feature has expired - hit the cache directly
          SelfClosingIterator(store.getFeatures.features) must beEmpty
          // verify feature has expired - hit the spatial index
          SelfClosingIterator(store.getFeatures(bbox).features) must beEmpty
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "clear on startup" >> {
      val params = Map("kafka.producer.clear" -> "true")
      val (producer, consumer, sft) = createStorePair("clear-on-startup", params)
      try {
        producer.createSchema(sft)
        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
        val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
        val f2 = ScalaSimpleFeature.create(sft, "do", "doe", 40, "2017-01-03T00:00:00.000Z", "POINT (10 10)")

        // initial write
        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach { f =>
            FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
            writer.write()
          }
        }
        eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must containTheSameElementsAs(Seq(f0, f1)))

        // new producer - clears on startup
        val producer2 = getStore(producer.config.catalog, 0, params)
        try {
          // write the third feature
          WithClose(producer2.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.copyToWriter(writer, f2, useProvidedFid = true)
            writer.write()
          }
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq mustEqual Seq(f2))
        } finally {
          producer2.dispose()
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support listeners" >> {
      val (producer, consumer, sft) = createStorePair("listeners")
      try {
        val id = "fid-0"
        val numUpdates = 1
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

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "migrate old kafka data store schemas" >> {
      val spec = "test:String,dtg:Date,*geom:Point:srid=4326"

      val path = s"geomesa/migrate/test/${paths.getAndIncrement()}"
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
    "Parse SSI tiers" >> {
      val key = KafkaDataStoreFactoryParams.IndexTiers.getName
      KafkaDataStoreFactory.parseSsiTiers(Collections.emptyMap()) mustEqual SizeSeparatedBucketIndex.DefaultTiers
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "foo")) mustEqual SizeSeparatedBucketIndex.DefaultTiers
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "1:2")) mustEqual Seq((1d, 2d))
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "1:2,3:4")) mustEqual Seq((1d, 2d), (3d, 4d))
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "3:4,1:2")) mustEqual Seq((1d, 2d), (3d, 4d))
    }
  }

  "KafkaFeatureSource" should {
    "handle Query instances with null TypeName (GeoServer querylayer extension implementation nuance)" >> {
      val (p, c, sft) = createStorePair("querylayer")
      p.createSchema(sft)
      val fs = c.getFeatureSource(sft.getTypeName)
      val q = new Query(null, Filter.INCLUDE)
      def check = fs.getFeatures(q).features().close()
      // induce issue
      check must not throwA[NullPointerException]()
    }
  }

  step {
    logger.info("Stopping embedded kafka/zk")
    kafka.close()
    logger.info("Stopped embedded kafka/zk")
  }
}
