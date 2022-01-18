/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import kafka.admin.AdminUtils
import org.geotools.data.memory.MemoryDataStore
import org.geotools.data.{DataUtilities, Query, Transaction}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.lambda.InMemoryOffsetManager
import org.locationtech.geomesa.lambda.LambdaTestRunnerTest.{LambdaTest, TestClock}
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType

class KafkaStoreTest extends LambdaTest with LazyLogging {

  import scala.concurrent.duration._

  sequential

  skipAllUnless(SystemProperty("geomesa.lambda.kafka.test").option.exists(_.toBoolean))

  lazy val config = Map("bootstrap.servers" -> brokers)

  val namespaces = new AtomicInteger(0)

  def newNamespace(): String = s"ks-test-${namespaces.getAndIncrement()}"

  def createTopic(ns: String, zookeepers: String, sft: SimpleFeatureType): Unit = {
    val topic = KafkaStore.topic(ns, sft)
    KafkaStore.withZk(zookeepers)(zk => AdminUtils.createTopic(zk, topic, 2, 1))
    logger.trace(s"created topic $topic")
  }

  step {
    logger.info("KafkaStoreTest starting")
  }

  // TODO tests fail intermittently - seems to be due to consumers hanging on retrieving messages?

  "TransientStore" should {
    "synchronize features among stores" in {
      implicit val clock = new TestClock()
      val ns = newNamespace()
      val sft = SimpleFeatureTypes.createType(ns, "name:String,dtg:Date,*geom:Point:srid=4326")
      val feature = ScalaSimpleFeature.create(sft, "0", "0", "2017-06-05T00:00:00.000Z", "POINT (45 50)")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true))

      createTopic(ns, zookeepers, sft)
      val ds = new MemoryDataStore()

      try {
        ds.createSchema(sft)
        val om = new InMemoryOffsetManager
        WithClose(KafkaStore.producer(config)) { producer =>
          def newStore(): KafkaStore =
            new KafkaStore(ds, sft, None, om, producer, config, LambdaConfig(zookeepers, ns, 2, 1, Duration(1000, "ms"), persist = true))
          WithClose(newStore(), newStore()) { (store1, store2) =>
            store1.write(feature)
            producer.flush()
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()).toSeq must beEqualTo(Seq(feature)))
            }
          }
          WithClose(newStore(), newStore()) { (store1, store2) =>
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()).toSeq must beEqualTo(Seq(feature)))
            }
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

      createTopic(ns, zookeepers, sft)
      val ds = new MemoryDataStore()

      try {
        ds.createSchema(sft)
        val om = new InMemoryOffsetManager
        WithClose(KafkaStore.producer(config)) { producer =>
          def newStore(): KafkaStore =
            new KafkaStore(ds, sft, None, om, producer, config, LambdaConfig(zookeepers, ns, 2, 1, Duration(1000, "ms"), persist = true))
          WithClose(newStore(), newStore()) { (store1, store2) =>
            store1.write(feature)
            producer.flush()
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()).toSeq must beEqualTo(Seq(feature)))
            }
            // run once with nothing expired
            store1.persist()
            foreach(Seq(store1, store2)) { store =>
              SelfClosingIterator(store.read()).toSeq mustEqual Seq(feature)
            }
            // move the clock forward and run again
            clock.tick = 2000
            store1.persist()
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()) must beEmpty)
            }
            val persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
            persisted.map(DataUtilities.encodeFeature) mustEqual Seq(DataUtilities.encodeFeature(feature))
          }
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

      createTopic(ns, zookeepers, sft)
      val ds = new MemoryDataStore()

      try {
        ds.createSchema(sft)
        val om = new InMemoryOffsetManager
        WithClose(KafkaStore.producer(config)) { producer =>
          def newStore(): KafkaStore =
            new KafkaStore(ds, sft, None, om, producer, config, LambdaConfig(zookeepers, ns, 2, 1, Duration(1000, "ms"), persist = true))
          WithClose(newStore(), newStore()) { (store1, store2) =>
            store1.write(feature1)
            store2.write(feature2)
            producer.flush()
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()).toSeq must containTheSameElementsAs(Seq(feature1, feature2)))
            }
            // move forward the clock to simulate an update
            clock.tick = 1
            store1.write(update2)
            store2.write(update1)
            producer.flush()
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()).toSeq must containTheSameElementsAs(Seq(update1, update2)))
            }
            clock.tick = 2
            store1.delete(update1)
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()).toSeq must beEqualTo(Seq(update2)))
            }
            // move the clock forward and run persistence
            clock.tick = 2000
            store1.persist()
            var persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
            persisted.map(DataUtilities.encodeFeature) mustEqual Seq(update2).map(DataUtilities.encodeFeature)

            store1.delete(update2)
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()).toSeq must beEmpty)
            }
            // move the clock forward and run persistence
            clock.tick = 4000
            store2.persist()
            foreach(Seq(store1, store2)) { store =>
              SelfClosingIterator(store.read()) must beEmpty
            }
            persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
            persisted must beEmpty
          }
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

      createTopic(ns, zookeepers, sft)
      val ds = new MemoryDataStore()

      try {
        ds.createSchema(sft)
        val om = new InMemoryOffsetManager
        WithClose(KafkaStore.producer(config)) { producer =>
          def newStore(): KafkaStore =
            new KafkaStore(ds, sft, None, om, producer, config, LambdaConfig(zookeepers, ns, 2, 1, Duration(1000, "ms"), persist = true))
          WithClose(newStore(), newStore()) { (store1, store2) =>
            store1.write(feature1)
            producer.flush()
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()).toSeq must beEqualTo(Seq(feature1)))
            }
            // move forward the clock to simulate an update
            // the first feature is expired, but the update is not
            clock.tick = 2000
            store2.write(update1)
            producer.flush()
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()).toSeq must beEqualTo(Seq(update1)))
            }
            // run persistence
            store1.persist()
            // ensure nothing was persisted
            var persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
            persisted must beEmpty
            // ensure non-expired feature still comes back
            foreach(Seq(store1, store2)) { store =>
              SelfClosingIterator(store.read()).toSeq mustEqual Seq(update1)
            }
            // move the clock forward and run persistence
            clock.tick = 4000
            store2.persist()
            foreach(Seq(store1, store2)) { store =>
              eventually(40, 100.millis)(SelfClosingIterator(store.read()) must beEmpty)
            }
            persisted = SelfClosingIterator(ds.getFeatureReader(new Query(ns), Transaction.AUTO_COMMIT)).toSeq
            persisted.map(DataUtilities.encodeFeature) mustEqual Seq(update1).map(DataUtilities.encodeFeature)
          }
        }
      } finally {
        ds.dispose()
      }
    }
  }

  step {
    logger.info("KafkaStoreTest complete")
  }
}
