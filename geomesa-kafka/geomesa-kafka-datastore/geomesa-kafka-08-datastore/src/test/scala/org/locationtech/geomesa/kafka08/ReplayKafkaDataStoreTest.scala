/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08

import java.io.Serializable
import java.{util => ju}

import kafka.admin.AdminUtils
import kafka.producer.{Producer, ProducerConfig}
import org.I0Itec.zkclient.ZkClient
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.{DataStore, Query}
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka._
import org.locationtech.geomesa.kafka08.KafkaDataStoreFactoryParams._
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.{After, Specification}
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

/** An integration test for the [[KafkaDataStore]] replay functionality.  There is no separate
  * ReplayKafkaDataStore.
  *
  */
@RunWith(classOf[JUnitRunner])
class ReplayKafkaDataStoreTest
  extends Specification
  with HasEmbeddedKafka
  with SimpleFeatureMatchers {

  import KafkaConsumerTestData._

  // skip embedded kafka tests unless explicitly enabled, they often fail randomly
  skipAllUnless(sys.props.get(SYS_PROP_RUN_TESTS).exists(_.toBoolean))

  val zkPath = "/kafkaDS/test"
  val sftName = sft.getTypeName

  var dataStore: DataStore = null
  var liveSFT: SimpleFeatureType = null

  "KafkaDataStore" should {
    // this is not the focus of this test, just the setup
    // the KafkaDataStoreTest covers SFT creation and sending in more detail
    "allowCreation of SFT and sending of messages" >> {

      dataStore = createDataStore
      liveSFT = KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)

      dataStore.createSchema(liveSFT)

      val topic = KafkaDataStoreHelper.extractTopic(liveSFT)
      topic must beSome(contain(liveSFT.getTypeName))

      val zkUtils = KafkaUtils08.createZkUtils(zkConnect, Int.MaxValue, Int.MaxValue)
      try {
        zkUtils.topicExists(topic.get) must beTrue
      } finally {
        zkUtils.close()
      }

      sendMessages(liveSFT)

      success
    }
  }

  step {
    // wait for above test to complete before moving on
    success("setup complete")
  }

  "KafkaDataStore via a replay consumer" should {

    "select the most recent version within the replay window when no message time is given" in
      new ReplayContext {

      val (replayType, fs) = createReplayFeatureSource(10000, 12000, 1000)
      fs.isInstanceOf[ReplayKafkaConsumerFeatureSource] must beTrue

      val features = featuresToList(fs.getFeatures)

      features must haveSize(3)
      features must containFeatures(expect(replayType, 12000L, track0v1, track1v0, track3v2))
    }

    "use the message time when given" in new ReplayContext {

      val (replayType, fs) = createReplayFeatureSource(10000, 20000, 1000)

      "in a filter" >> {
        val filter = ReplayTimeHelper.toFilter(new Instant(13000))

        val features = featuresToList(fs.getFeatures(filter))

        features must haveSize(2)
        features must containFeatures(expect(replayType, 13000L, track1v1, track2v0))
      }

      "in a query" >> {
        val filter = ReplayTimeHelper.toFilter(new Instant(13000))
        val query = new Query()
        query.setFilter(filter)

        val features = featuresToList(fs.getFeatures(filter))

        features must haveSize(2)
        features must containFeatures(expect(replayType, 13000L, track1v1, track2v0))
      }
    }

    "find messages with the same time" in new ReplayContext {

      val (replayType, fs) = createReplayFeatureSource(10000, 15000, 1000)

      val filter = ReplayTimeHelper.toFilter(new Instant(12000))

      val features = featuresToList(fs.getFeatures(filter))

      features must haveSize(3)
      features must containFeatures(expect(replayType, 12000L, track0v1, track1v0, track3v2))
    }

    "handle the case where there is no data" >> {

      "because the window is before all data" in new ReplayContext {
        val (replayType, fs) = createReplayFeatureSource(5000, 7000, 1000)

        val features = featuresToList(fs.getFeatures)
        features must haveSize(0)
      }

      "because the window is after all data" in new ReplayContext {
        val (replayType, fs) = createReplayFeatureSource(25000, 27000, 1000)

        val features = featuresToList(fs.getFeatures)
        features must haveSize(0)
      }

      "because there is no data at the requested time" in new ReplayContext {
        val (replayType, fs) = createReplayFeatureSource(10000, 20000, 500)

        val filter = ReplayTimeHelper.toFilter(new Instant(12500))
        val features = featuresToList(fs.getFeatures(filter))

        features must haveSize(0)
      }
    }
  }

  step {
    // wait for above test to complete before moving on
    success("main tests complete")
  }

  "KafkaDataStore" should {

    "allow simple feature types to be cleaned up" >> {
      val names = dataStore.getTypeNames
      names must haveSize(1)
      names.head mustEqual liveSFT.getTypeName

      dataStore.removeSchema(liveSFT.getTypeName)
      dataStore.getTypeNames must haveSize(0)

      dataStore.getSchema(liveSFT.getTypeName) must throwA[Exception]
    }
  }

  step {
    shutdown()
    success("shutdown complete")
  }

  def featuresToList(sfc: SimpleFeatureCollection): List[SimpleFeature] = {
    val iter: RichSimpleFeatureIterator = sfc.features()
    val features = iter.toList
    iter.close()
    features
  }

  def createDataStore: DataStore = {
    val props = Map(
      KAFKA_BROKER_PARAM.key -> brokerConnect,
      ZOOKEEPERS_PARAM.key -> zkConnect,
      ZK_PATH.key -> zkPath,
      IS_PRODUCER_PARAM.key -> false.asInstanceOf[Serializable]
    )

    new KafkaDataStoreFactory().createDataStore(props)
  }

  def sendMessages(sft: SimpleFeatureType): Unit = {
    val props = new ju.Properties()
    props.put(KafkaUtils08.brokerParam, brokerConnect)
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    val kafkaProducer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(props))

    val encoder = new KafkaGeoMessageEncoder(sft)
    val topic = KafkaFeatureConfig(sft).topic

    messages.foreach(msg => kafkaProducer.send(encoder.encodeMessage(topic, msg)))
  }

  trait ReplayContext extends After {

    private var toCleanup = Seq.empty[SimpleFeatureType]

    def createReplayFeatureSource(start: Long, end: Long, readBehind: Long): (SimpleFeatureType, SimpleFeatureSource) = {
      val rc = ReplayConfig(start, end, readBehind)
      val replaySFT = KafkaDataStoreHelper.createReplaySFT(liveSFT, rc)
      dataStore.createSchema(replaySFT)

      val fs = dataStore.getFeatureSource(replaySFT.getTypeName)

      toCleanup +:= replaySFT

      (replaySFT, fs)
    }

    override def after: Unit = {
      toCleanup.foreach(sft => dataStore.removeSchema(sft.getTypeName))
    }
  }
}
