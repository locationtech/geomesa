/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.geomesa.testcontainers.AccumuloContainer
import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.{Filter, Id}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.lambda.LambdaContainerTest.TestClock
import org.locationtech.geomesa.lambda.data.LambdaDataStore
import org.locationtech.geomesa.lambda.data.LambdaDataStore.{LambdaConfig, PersistenceConfig}
import org.locationtech.geomesa.lambda.{InMemoryOffsetManager, LambdaContainerTest}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.{Collections, Properties}

class KafkaStoreTest extends LambdaContainerTest {

  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  sequential

  lazy val config = Map("bootstrap.servers" -> kafka.getBootstrapServers)

  lazy val zookeepers = AccumuloContainer.getInstance().getZookeepers

  lazy val namespaces = new AtomicInteger(0)

  def newNamespace(): String = s"ks-test-${namespaces.getAndIncrement()}"

  def createTopic(ns: String, sft: SimpleFeatureType): Unit = {
    val topic = LambdaDataStore.topic(sft, ns)
    val props = new Properties()
    props.put("bootstrap.servers", kafka.getBootstrapServers)
    WithClose(AdminClient.create(props)) { admin =>
      admin.createTopics(Collections.singletonList(new NewTopic(topic, 2, 1.toShort))).all().get
    }
    logger.trace(s"created topic $topic")
  }

  // TODO tests fail intermittently - seems to be due to consumers hanging on retrieving messages?

  "TransientStore" should {
    "synchronize features among stores" in {
      implicit val clock: TestClock = new TestClock()
      val ns = newNamespace()
      val sft = SimpleFeatureTypes.createType(ns, "name:String,dtg:Date,*geom:Point:srid=4326")
      val feature = ScalaSimpleFeature.create(sft, "0", "0", "2017-06-05T00:00:00.000Z", "POINT (45 50)")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true))

      createTopic(ns, sft)
      WithClose(new DataStoreShell(sft)) { ds =>
        val om = new InMemoryOffsetManager
        def newStore(): KafkaStore = {
          new KafkaStore(ds, sft, None, om,
            LambdaConfig(zookeepers, ns, config, config, 2, 1, Some(PersistenceConfig(Duration(1, "s"), 100)), Duration(10, TimeUnit.SECONDS)))
        }

        WithClose(newStore(), newStore()) { (store1, store2) =>
          store1.write(feature)
          store1.flush()
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq mustEqual Seq(feature))
          }
        }
        WithClose(newStore(), newStore()) { (store1, store2) =>
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq mustEqual Seq(feature))
          }
        }
      }
    }

    "correctly expire features" in {
      implicit val clock: TestClock = new TestClock()
      val ns = newNamespace()
      val sft = SimpleFeatureTypes.createType(ns, "name:String,dtg:Date,*geom:Point:srid=4326")
      val feature = ScalaSimpleFeature.create(sft, "0", "0", "2017-06-05T00:00:00.000Z", "POINT (45 50)")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true))

      createTopic(ns, sft)

      WithClose(new DataStoreShell(sft)) { ds =>
        val om = new InMemoryOffsetManager
        def newStore(): KafkaStore = {
          new KafkaStore(ds, sft, None, om,
            LambdaConfig(zookeepers, ns, config, config, 2, 1, Some(PersistenceConfig(Duration(1, "s"), 100)), Duration(10, TimeUnit.SECONDS)))
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
          ds.all() mustEqual Seq(feature)
        }
      }
    }

    "process updates and deletes" in {
      implicit val clock: TestClock = new TestClock()
      val ns = newNamespace()
      val sft = SimpleFeatureTypes.createType(ns, "name:String,dtg:Date,*geom:Point:srid=4326")
      val feature1 = ScalaSimpleFeature.create(sft, "0", "0", "2017-06-05T00:00:00.000Z", "POINT (45 50)")
      val feature2 = ScalaSimpleFeature.create(sft, "1", "0", "2017-06-06T00:00:00.000Z", "POINT (46 50)")
      val update1 = ScalaSimpleFeature.create(sft, "0", "1", "2017-06-05T00:00:01.000Z", "POINT (45 51)")
      val update2 = ScalaSimpleFeature.create(sft, "1", "1", "2017-06-06T00:00:01.000Z", "POINT (46 51)")

      Seq(feature1, feature2, update1, update2).foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true)))

      createTopic(ns, sft)
      WithClose(new DataStoreShell(sft)) { ds =>
        val om = new InMemoryOffsetManager
        def newStore(): KafkaStore = {
          new KafkaStore(ds, sft, None, om,
            LambdaConfig(zookeepers, ns, config, config, 2, 1, Some(PersistenceConfig(Duration(1, "s"), 100)), Duration(10, TimeUnit.SECONDS)))
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
          ds.all() mustEqual Seq(update2)

          store1.delete(update2)
          foreach(Seq(store1, store2)) { store =>
            eventually(40, 100.millis)(SelfClosingIterator(store.read().iterator()).toSeq must beEmpty)
          }
          // verify the delete was persisted to the backing store
          eventually(40, 100.millis)(ds.all() must beEmpty)
          // verify running persistence doesn't re-add the feature
          // move the clock forward and run persistence
          clock.tick = 4000
          store2.persist()
          foreach(Seq(store1, store2)) { store =>
            SelfClosingIterator(store.read().iterator()) must beEmpty
          }
          ds.all() must beEmpty
        }
      }
    }

    "only persist final updates" in {
      implicit val clock: TestClock = new TestClock()
      val ns = newNamespace()
      val sft = SimpleFeatureTypes.createType(ns, "name:String,dtg:Date,*geom:Point:srid=4326")
      val feature1 = ScalaSimpleFeature.create(sft, "0", "0", "2017-06-05T00:00:00.000Z", "POINT (45 50)")
      val update1 = ScalaSimpleFeature.create(sft, "0", "1", "2017-06-05T00:00:01.000Z", "POINT (45 51)")

      Seq(feature1, update1).foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true)))

      createTopic(ns, sft)
      WithClose(new DataStoreShell(sft)) { ds =>
        val om = new InMemoryOffsetManager
        def newStore(): KafkaStore = {
          new KafkaStore(ds, sft, None, om,
            LambdaConfig(zookeepers, ns, config, config, 2, 1, Some(PersistenceConfig(Duration(1, "s"), 100)), Duration(10, TimeUnit.SECONDS)))
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
          ds.all() must beEmpty
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
          ds.all() mustEqual Seq(update1)
        }
      }
    }
  }

  // shell class that only implements the methods used by KafkaStore, for testing
  class DataStoreShell(sft: SimpleFeatureType) extends DataStore {

    private val features = new ConcurrentHashMap[String, SimpleFeature]()

    def all(): Seq[SimpleFeature] = features.values().asScala.toSeq

    override def getTypeNames: Array[String] = Array(sft.getTypeName)
    override def getSchema(typeName: String): SimpleFeatureType = sft
    override def getSchema(name: Name): SimpleFeatureType = sft

    override def getFeatureWriter(
      typeName: String, filter: Filter, transaction: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
      new SimpleFeatureWriter {
        private val updates = filter match {
          case ids: Id => ids.getIDs.iterator.asScala.flatMap(i => Option(features.get(i.toString)))
          case _ => throw new UnsupportedOperationException("Only modify by ID is supported")
        }
        private var sf: SimpleFeature = _
        override def getFeatureType: SimpleFeatureType = sft
        override def hasNext: Boolean = updates.hasNext
        override def next(): SimpleFeature = {
          sf = updates.next()
          sf
        }
        override def remove(): Unit = features.remove(sf.getID)
        override def write(): Unit = {} // we passed in a live reference so should already be updated
        override def close(): Unit = {}
      }
    }

    override def getFeatureWriterAppend(typeName: String, transaction: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
      new SimpleFeatureWriter {
        private var sf: SimpleFeature = _
        override def getFeatureType: SimpleFeatureType = sft
        override def hasNext: Boolean = false
        override def next(): SimpleFeature = {
          sf = new ScalaSimpleFeature(sft, "")
          sf
        }
        override def remove(): Unit = {}
        override def write(): Unit = features.put(sf.getID, sf)
        override def close(): Unit = {}
      }
    }

    override def dispose(): Unit = {}

    override def createSchema(featureType: SimpleFeatureType): Unit = throw new NotImplementedError()
    override def updateSchema(typeName: String, featureType: SimpleFeatureType): Unit = throw new NotImplementedError()
    override def removeSchema(typeName: String): Unit = throw new NotImplementedError()
    override def getFeatureSource(typeName: String): SimpleFeatureSource = throw new NotImplementedError()
    override def getFeatureSource(typeName: Name): SimpleFeatureSource = throw new NotImplementedError()
    override def getFeatureReader(query: Query, transaction: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] =
      throw new NotImplementedError()
    override def getFeatureWriter(typeName: String, transaction: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] =
      throw new NotImplementedError()
    override def getLockingManager: LockingManager = throw new NotImplementedError()
    override def getInfo: ServiceInfo = throw new NotImplementedError()
    override def updateSchema(typeName: Name, featureType: SimpleFeatureType): Unit = throw new NotImplementedError()
    override def removeSchema(typeName: Name): Unit = throw new NotImplementedError()
    override def getNames: java.util.List[Name] = throw new NotImplementedError()
  }
}
