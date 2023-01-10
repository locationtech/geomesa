/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.geotools.api.data.{Query, Transaction}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.DataUtilities
import org.geotools.data.memory.MemoryDataStore
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.lambda.LambdaContainerTest.TestClock
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.{InMemoryOffsetManager, LambdaContainerTest}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.runner.JUnitRunner

import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Properties}

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
@RunWith(classOf[JUnitRunner])
class KafkaStoreTest extends LambdaContainerTest {
=======
class KafkaStoreTest extends LambdaTest with LazyLogging {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
@RunWith(classOf[JUnitRunner])
class KafkaStoreTest extends LambdaContainerTest {
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)

  import scala.concurrent.duration._

  sequential

  lazy val config = Map("bootstrap.servers" -> brokers)

  val namespaces = new AtomicInteger(0)

  def newNamespace(): String = s"ks-test-${namespaces.getAndIncrement()}"

  def createTopic(ns: String, sft: SimpleFeatureType): Unit = {
    val topic = KafkaStore.topic(ns, sft)
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    WithClose(AdminClient.create(props)) { admin =>
      admin.createTopics(Collections.singletonList(new NewTopic(topic, 2, 1.toShort))).all().get
    }
    logger.trace(s"created topic $topic")
  }

  // TODO tests fail intermittently - seems to be due to consumers hanging on retrieving messages?

  "TransientStore" should {
    "synchronize features among stores" in {
      implicit val clock = new TestClock()
      val ns = newNamespace()
      val sft = SimpleFeatureTypes.createType(ns, "name:String,dtg:Date,*geom:Point:srid=4326")
      val feature = ScalaSimpleFeature.create(sft, "0", "0", "2017-06-05T00:00:00.000Z", "POINT (45 50)")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true))

      createTopic(ns, sft)
      val ds = new MemoryDataStore()

      try {
        ds.createSchema(sft)
        val om = new InMemoryOffsetManager
        def newStore(): KafkaStore = {
          new KafkaStore(ds, sft, None,
            LambdaConfig(zookeepers, ns, config, config, 2, 1, Duration(1000, "ms"), persist = true, om))
        }

        WithClose(newStore(), newStore()) { (store1, store2) =>
          store1.write(feature)
          store1.flush()
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must beEqualTo(Seq(feature)))
          }
        }
        WithClose(newStore(), newStore()) { (store1, store2) =>
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must beEqualTo(Seq(feature)))
          }
        }
      } finally {
        ds.dispose()
      }
    }

    "correctly expire features" in {
      implicit val clock = new TestClock()
      val ns = newNamespace()
      val sft = SimpleFeatureTypes.createType(ns, "name:String,dtg:Date,*geom:Point:srid=4326")
      val feature = ScalaSimpleFeature.create(sft, "0", "0", "2017-06-05T00:00:00.000Z", "POINT (45 50)")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true))

      createTopic(ns, sft)
      val ds = new MemoryDataStore()

      try {
        ds.createSchema(sft)
        val om = new InMemoryOffsetManager
        def newStore(): KafkaStore = {
          new KafkaStore(ds, sft, None,
            LambdaConfig(zookeepers, ns, config, config, 2, 1, Duration(1000, "ms"), persist = true, om))
        }
        WithClose(newStore(), newStore()) { (store1, store2) =>
          store1.write(feature)
          store1.flush()
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must beEqualTo(Seq(feature)))
          }
          // run once with nothing expired
          store1.persist()
          foreach(Seq(store1, store2)) { store =>
            SelfClosingIterator(store.read().iterator()).toSeq mustEqual Seq(feature)
          }
          // move the clock forward and run again
          clock.tick = 2000
          store1.persist()
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()) must beEmpty)
          }
          val persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
          persisted.map(DataUtilities.encodeFeature) mustEqual Seq(DataUtilities.encodeFeature(feature))
        }
      } finally {
        ds.dispose()
      }
    }

    "process updates and deletes" in {
      implicit val clock = new TestClock()
      val ns = newNamespace()
      val sft = SimpleFeatureTypes.createType(ns, "name:String,dtg:Date,*geom:Point:srid=4326")
      val feature1 = ScalaSimpleFeature.create(sft, "0", "0", "2017-06-05T00:00:00.000Z", "POINT (45 50)")
      val feature2 = ScalaSimpleFeature.create(sft, "1", "0", "2017-06-06T00:00:00.000Z", "POINT (46 50)")
      val update1 = ScalaSimpleFeature.create(sft, "0", "1", "2017-06-05T00:00:01.000Z", "POINT (45 51)")
      val update2 = ScalaSimpleFeature.create(sft, "1", "1", "2017-06-06T00:00:01.000Z", "POINT (46 51)")

      Seq(feature1, feature2, update1, update2).foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true)))

      createTopic(ns, sft)
      val ds = new MemoryDataStore()

      try {
        ds.createSchema(sft)
        val om = new InMemoryOffsetManager
        def newStore(): KafkaStore = {
          new KafkaStore(ds, sft, None,
            LambdaConfig(zookeepers, ns, config, config, 2, 1, Duration(1000, "ms"), persist = true, om))
        }
        WithClose(newStore(), newStore()) { (store1, store2) =>
          store1.write(feature1)
          store2.write(feature2)
          store1.flush()
          store2.flush()
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must containTheSameElementsAs(Seq(feature1, feature2)))
          }
          // move forward the clock to simulate an update
          clock.tick = 1
          store1.write(update2)
          store2.write(update1)
          store1.flush()
          store2.flush()
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must containTheSameElementsAs(Seq(update1, update2)))
          }
          clock.tick = 2
          store1.delete(update1)
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must beEqualTo(Seq(update2)))
          }
          // move the clock forward and run persistence
          clock.tick = 2000
          store1.persist()
          var persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
          persisted.map(DataUtilities.encodeFeature) mustEqual Seq(update2).map(DataUtilities.encodeFeature)

          store1.delete(update2)
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must beEmpty)
          }
          // move the clock forward and run persistence
          clock.tick = 4000
          store2.persist()
          foreach(Seq(store1, store2)) { store =>
            SelfClosingIterator(store.read().iterator()) must beEmpty
          }
          persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
          persisted must beEmpty
        }
      } finally {
        ds.dispose()
      }
    }

    "only persist final updates" in {
      implicit val clock = new TestClock()
      val ns = newNamespace()
      val sft = SimpleFeatureTypes.createType(ns, "name:String,dtg:Date,*geom:Point:srid=4326")
      val feature1 = ScalaSimpleFeature.create(sft, "0", "0", "2017-06-05T00:00:00.000Z", "POINT (45 50)")
      val update1 = ScalaSimpleFeature.create(sft, "0", "1", "2017-06-05T00:00:01.000Z", "POINT (45 51)")

      Seq(feature1, update1).foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true)))

      createTopic(ns, sft)
      val ds = new MemoryDataStore()

      try {
        ds.createSchema(sft)
        val om = new InMemoryOffsetManager
        def newStore(): KafkaStore = {
          new KafkaStore(ds, sft, None,
            LambdaConfig(zookeepers, ns, config, config, 2, 1, Duration(1000, "ms"), persist = true, om))
        }
        WithClose(newStore(), newStore()) { (store1, store2) =>
          store1.write(feature1)
          store1.flush()
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must beEqualTo(Seq(feature1)))
          }
          // move forward the clock to simulate an update
          // the first feature is expired, but the update is not
          clock.tick = 2000
          store2.write(update1)
          store2.flush()
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must beEqualTo(Seq(update1)))
          }
          // run persistence
          store1.persist()
          // ensure nothing was persisted
          var persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
          persisted must beEmpty
          // ensure non-expired feature still comes back
          foreach(Seq(store1, store2)) { store =>
            SelfClosingIterator(store.read().iterator()).toSeq mustEqual Seq(update1)
          }
          // move the clock forward and run persistence
          clock.tick = 4000
          store2.persist()
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()) must beEmpty)
          }
          persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
          persisted.map(DataUtilities.encodeFeature) mustEqual Seq(update1).map(DataUtilities.encodeFeature)
        }
      } finally {
        ds.dispose()
      }
    }
  }
}
