/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.kafka

import java.io.Serializable
import java.{util => ju}

import com.typesafe.scalalogging.slf4j.Logging
import kafka.admin.AdminUtils
import kafka.producer.{Producer, ProducerConfig}
import org.I0Itec.zkclient.ZkClient
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.{DataStore, Query}
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.KafkaDataStoreFactoryParams._
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ReplayKafkaDataStoreTest
  extends Specification
  with HasEmbeddedZookeeper
  with SimpleFeatureMatchers
  with Logging {

  import KafkaConsumerTestData._

  val zkPath = "/kafkaDS/test"
  val sftName = sft.getTypeName

  // lazy initialization to avoid blowing up test framework if initialization fails
  lazy val dataStore = createDataStore
  lazy val liveSFT = {
    val result = createLiveSFT
    sendMessages(result)
    result
  }

  "replay" should {

    "select the most recent version within the replay window when no message time is given" >> {

      val (replayType, fs) = createReplayFeatureSource(10000, 12000, 1000)
      fs.isInstanceOf[ReplayKafkaConsumerFeatureSource] must beTrue

      val features = featuresToList(fs.getFeatures)

      features must haveSize(3)
      features must containFeatures(expect(replayType, 12000L, track0v1, track1v0, track3v2))
    }

    "use the message time when given" >> {

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

    "find messages with the same time" >> {

      val (replayType, fs) = createReplayFeatureSource(10000, 15000, 1000)

      val filter = ReplayTimeHelper.toFilter(new Instant(12000))

      val features = featuresToList(fs.getFeatures(filter))

      features must haveSize(3)
      features must containFeatures(expect(replayType, 12000L, track0v1, track1v0, track3v2))
    }
  }

  step {
    shutdown()
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

  def createLiveSFT: SimpleFeatureType = {
    val prepped = KafkaDataStoreHelper.prepareForLive(sft, zkPath)
    dataStore.createSchema(prepped)

    val topic = KafkaDataStoreHelper.extractTopic(prepped)
    topic must beSome

    val zkClient = new ZkClient(zkConnect)
    try {
      AdminUtils.topicExists(zkClient, topic.get) must beTrue
    } finally {
      zkClient.close()
    }

    prepped
  }

  def sendMessages(sft: SimpleFeatureType): Unit = {
    val props = new ju.Properties()
    props.put("metadata.broker.list", brokerConnect)
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    val kafkaProducer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(props))

    val encoder = new KafkaGeoMessageEncoder(sft)
    val topic = KafkaFeatureConfig(sft).topic

    logger.info("Sending {} messages to Kafka (zoo={}) (topic={})",
      messages.size.asInstanceOf[AnyRef], zkConnect, topic)

    messages.foreach(msg => kafkaProducer.send(encoder.encodeMessage(topic, msg)))

    logger.info("Completed message send", messages.size.asInstanceOf[AnyRef], zkConnect)
  }

  def createReplayFeatureSource(start: Long, end: Long, readBehind: Long): (SimpleFeatureType, SimpleFeatureSource) = {
    val rc = ReplayConfig(start, end, readBehind)
    val replaySFT = KafkaDataStoreHelper.prepareForReplay(liveSFT, rc)
    dataStore.createSchema(replaySFT)

    val fs = dataStore.getFeatureSource(replaySFT.getTypeName)
    (replaySFT, fs)
  }
}
