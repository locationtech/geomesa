/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.geotools.data.store.{ContentDataStore, ContentEntry}
import org.geotools.feature.NameImpl
import org.joda.time.{Duration, Instant}
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.{KafkaDataStoreHelper, ReplayConfig}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.{After, Specification}
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreSchemaManagerTest
  extends Specification
  with HasEmbeddedKafka {

  // skip embedded kafka tests unless explicitly enabled, they often fail randomly
  skipAllUnless(sys.props.get(SYS_PROP_RUN_TESTS).exists(_.toBoolean))

  // todo: missing tests -
  // todo    test general exception handling (use zk mock for this?)

  "createSchema" should {

    "fail if SFT is not a Streaming or Replay type" in new ZkContext(zkConnect) {

      val typename = "test-create-no-topic"
      val sft = createSFT(typename)

      val datastore = new TestDataStore(zkConnect, zkPath)
      datastore.createSchema(sft) must throwA[IllegalArgumentException]
    }

    "with a Streaming SFT" should {

      "result in exactly one schema node and exactly one topic node" in new ZkContext(zkConnect) {

        val datastore = new TestDataStore(zkConnect, zkPath)
        val typename = "test-create-live"
        val sft = createStreamingSFT(typename)
        datastore.createSchema(sft)

        zkClient.countChildren(zkPath) mustEqual 1

        val zChildren = zkClient.getChildren(zkPath)
        zChildren.size() mustEqual 1

        zChildren.get(0) mustEqual typename
        val sPath = datastore.getSchemaPath(typename)
        val encoded = zkClient.readData[String](sPath)
        encoded mustEqual SimpleFeatureTypes.encodeType(sft, includeUserData = true)

        val sChildren = zkClient.getChildren(sPath)
        sChildren.size() mustEqual 1
        sChildren.get(0) mustEqual "Topic"

        val topic = zkClient.readData[String](datastore.getTopicPath(typename))
        zkUtils.topicExists(topic) must beTrue
      }
    }

    "with a Replay SFT" should {

      "result in exactly one schema node and exactly one topic node and one replay node" in new ZkContext(zkConnect) {

        val datastore = new TestDataStore(zkConnect, zkPath)
        val typename = "test-create-replay"
        val sft = createReplaySFT(typename)
        datastore.createSchema(sft)

        zkClient.countChildren(zkPath) mustEqual 1

        val zChildren = zkClient.getChildren(zkPath)
        zChildren.size() mustEqual 1

        val replayTypename = zChildren.get(0)
        replayTypename mustEqual sft.getTypeName

        val sPath = datastore.getSchemaPath(replayTypename)
        val encoded = zkClient.readData[String](sPath)
        encoded mustEqual SimpleFeatureTypes.encodeType(sft, includeUserData = true)

        val sChildren = zkClient.getChildren(sPath).asScala
        sChildren.size mustEqual 2
        sChildren must contain("Topic", "ReplayConfig")

        val topic = zkClient.readData[String](datastore.getTopicPath(replayTypename))
        zkUtils.topicExists(topic) must beTrue

        val encodeReplayConfig = zkClient.readData[String](datastore.getReplayConfigPath(replayTypename))
        ReplayConfig.decode(encodeReplayConfig) must beSome(replayConfig)
      }
    }

    "fail if type already created" in new ZkContext(zkConnect) {

      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-live-already-exists"
      val sft = createStreamingSFT(typename)
      datastore.createSchema(sft)

      val sft2 = createStreamingSFT(typename)
      datastore.createSchema(sft2) must throwA[IllegalArgumentException]
    }

  }

  "getFeatureConfig" should {

    "throws exception when type doesn't exists" in new ZkContext(zkConnect) {

      val datastore = new TestDataStore(zkConnect, zkPath)
      val typeName = "test-get-doesNotExist"

      datastore.getFeatureConfig(typeName) must throwA[RuntimeException]
    }

    "retrieve an existing schema" >> {

      "for a Streaming SFT" in new ZkContext(zkConnect) {
        val datastore = new TestDataStore(zkConnect, zkPath)
        val typename = "test-get-streaming"
        val sft = createStreamingSFT(typename)
        val topic = KafkaDataStoreHelper.extractTopic(sft).get
        datastore.createSchema(sft)

        val fc = datastore.getFeatureConfig(sft.getTypeName)
        fc must not(beNull)

        fc.topic mustEqual topic
        fc.replayConfig must beNone
        fc.sft mustEqual sft
        fc.sft.getUserData mustEqual sft.getUserData
      }

      "for a Replay SFT" in new ZkContext(zkConnect) {
        val datastore = new TestDataStore(zkConnect, zkPath)
        val typename = "test-get-replay"
        val sft = createReplaySFT(typename)
        val topic = KafkaDataStoreHelper.extractTopic(sft).get
        datastore.createSchema(sft)

        val fc = datastore.getFeatureConfig(sft.getTypeName)
        fc must not(beNull)

        fc.topic mustEqual topic
        fc.replayConfig must beSome(replayConfig)
        fc.sft mustEqual sft
        fc.sft.getUserData mustEqual sft.getUserData
      }
    }
  }

  "getNames" should {

    "return all created type names" >> {

      "when there are none" in new ZkContext(zkConnect) {

        val datastore = new TestDataStore(zkConnect, zkPath)

        val names = datastore.getNames()
        names must haveSize(0)
      }

      "when there is one" in new ZkContext(zkConnect) {

        val datastore = new TestDataStore(zkConnect, zkPath)
        val typename = "test-getnames-single"

        val liveSft = createStreamingSFT(typename)
        datastore.createSchema(liveSft)

        val expected = List(liveSft.getTypeName).map(name => new NameImpl(name): Name)

        val names = datastore.getNames().asScala
        names must containTheSameElementsAs(expected)
      }

      "when there are multiple" in new ZkContext(zkConnect) {

        val datastore = new TestDataStore(zkConnect, zkPath)
        val typename = "test-getnames-multiple"

        val liveSft = createStreamingSFT(typename)
        datastore.createSchema(liveSft)

        val replaySft = KafkaDataStoreHelper.createReplaySFT(liveSft, replayConfig)
        datastore.createSchema(replaySft)

        val expected = List(liveSft.getTypeName, replaySft.getTypeName).map(name => new NameImpl(name): Name)

        val names = datastore.getNames().asScala
        names must containTheSameElementsAs(expected)
      }

    }

  }

  "removeSchema" should {

    "remove the specified schema" >> {

      "when given a String" in new ZkContext(zkConnect) {

        //create two schemas (one replay, one live)
        val datastore = new TestDataStore(zkConnect, zkPath)
        val typename = "test-remove-string"

        val liveSFT = createStreamingSFT(typename)
        datastore.createSchema(liveSFT)

        val replaySFT = KafkaDataStoreHelper.createReplaySFT(liveSFT, replayConfig)
        datastore.createSchema(replaySFT)

        // verify there are two
        datastore.getNames must haveSize(2)

        // now remove the replay
        datastore.removeSchema(replaySFT.getTypeName)

        datastore.getNames must haveSize(1)

        datastore.getFeatureConfig(replaySFT.getTypeName) must throwA[RuntimeException]
        datastore.getFeatureConfig(liveSFT.getTypeName) must not(beNull)

        zkClient.exists(datastore.getSchemaPath(replaySFT.getTypeName)) must beFalse
        zkClient.exists(datastore.getSchemaPath(liveSFT.getTypeName)) must beTrue

        val topic = KafkaDataStoreHelper.extractTopic(liveSFT).get
        zkUtils.topicExists(topic) must beTrue and (
          zkClient.exists(ZkUtils.getDeleteTopicPath(topic)) must beFalse)
      }

      "when given a Name" in new ZkContext(zkConnect) {

        //create two schemas (one replay, one live)
        val datastore = new TestDataStore(zkConnect, zkPath)
        val typename = "test-remove-string"

        val liveSFT = createStreamingSFT(typename)
        datastore.createSchema(liveSFT)

        val replaySFT = KafkaDataStoreHelper.createReplaySFT(liveSFT, replayConfig)
        datastore.createSchema(replaySFT)

        // verify there are two
        datastore.getNames must haveSize(2)

        // now remove the replay
        datastore.removeSchema(replaySFT.getName)

        datastore.getNames must haveSize(1)

        datastore.getFeatureConfig(replaySFT.getTypeName) must throwA[RuntimeException]
        datastore.getFeatureConfig(liveSFT.getTypeName) must not(beNull)

        zkClient.exists(datastore.getSchemaPath(replaySFT.getTypeName)) must beFalse
        zkClient.exists(datastore.getSchemaPath(liveSFT.getTypeName)) must beTrue

        val topic = KafkaDataStoreHelper.extractTopic(liveSFT).get
        zkUtils.topicExists(topic) must beTrue and (
          zkClient.exists(ZkUtils.getDeleteTopicPath(topic)) must beFalse)
      }

      "delete the topic when removing a Streaming SFT" in new ZkContext(zkConnect) {

        val datastore = new TestDataStore(zkConnect, zkPath)
        val typename = "test-remove-string"

        val liveSFT = createStreamingSFT(typename)
        datastore.createSchema(liveSFT)

        // now remove the replay
        datastore.removeSchema(liveSFT.getTypeName)

        datastore.getNames must haveSize(0)

        datastore.getFeatureConfig(liveSFT.getTypeName) must throwA[RuntimeException]

        zkClient.exists(datastore.getSchemaPath(liveSFT.getTypeName)) must beFalse

        // the topic should no longer exist or at a minimum be marked for deletion
        val topic = KafkaDataStoreHelper.extractTopic(liveSFT).get
        zkUtils.topicExists(topic) must beFalse or (
          zkClient.exists(ZkUtils.getDeleteTopicPath(topic)) must beTrue)
      }
    }
  }

  "getLiveFeatureType" should {

    "retrieve the appropriate original schema" in new ZkContext(zkConnect) {

      //create two schemas (one replay, one live)
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-getlive"

      val liveSFT = createStreamingSFT(typename)
      datastore.createSchema(liveSFT)

      val replaySFT = KafkaDataStoreHelper.createReplaySFT(liveSFT, replayConfig)
      datastore.createSchema(replaySFT)

      val result = datastore.getLiveFeatureType(replaySFT)
      result must beSome(liveSFT)

      val origSft = result.get
      origSft.getUserData mustEqual liveSFT.getUserData
    }

    "return None if the original schema doesn't exist" in new ZkContext(zkConnect) {

      //create two schemas (one replay, one live)
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-getlive-noexists"

      val liveSFT = createStreamingSFT(typename)
      datastore.createSchema(liveSFT)

      val replaySFT = KafkaDataStoreHelper.createReplaySFT(liveSFT, replayConfig)
      datastore.createSchema(replaySFT)

      datastore.removeSchema(liveSFT.getTypeName)
      datastore.getLiveFeatureType(replaySFT) must beNone
    }

    "return None if not a replay schema" in new ZkContext(zkConnect) {

      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-getlive-notreplay"

      val liveSFT = createStreamingSFT(typename)
      datastore.createSchema(liveSFT)

      datastore.getLiveFeatureType(liveSFT) must beNone
    }
  }

  step {
    shutdown()
  }
}

class TestDataStore(override val zookeepers: String,
                    override val zkPath: String)
  extends ContentDataStore with KafkaDataStoreSchemaManager with LazyLogging {

  override val partitions: Int = 1
  override val replication: Int = 1

  override def createFeatureSource(entry: ContentEntry) = {
    throw new UnsupportedOperationException(
      "called TestDataStore.createFeatureSource() - this should not have happened!!!")
  }

  override def createTypeNames() = getNames()
}

class ZkContext(val zkConnect: String) extends After with LazyLogging {

  val schema = "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"
  lazy val replayConfig = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))

  val zkUtils = KafkaUtils08.createZkUtils(zkConnect, Int.MaxValue, Int.MaxValue)
  val zkClient = zkUtils.zkClient
  val zkPath = createRandomZkNode(zkClient)
  logger.trace(s"created $zkPath")

  def createSFT(typeName: String): SimpleFeatureType = {
    SimpleFeatureTypes.createType(typeName, schema)
  }

  def createStreamingSFT(typeName: String): SimpleFeatureType = {
    KafkaDataStoreHelper.createStreamingSFT(createSFT(typeName), zkPath)
  }

  def createReplaySFT(typeName: String, rc: ReplayConfig = replayConfig): SimpleFeatureType = {
    KafkaDataStoreHelper.createReplaySFT(createStreamingSFT(typeName), rc)
  }

  override def after = {
    logger.trace(s"cleaning up $zkPath")
    zkClient.deleteRecursive(zkPath)
//    zkClient.close()
  }

  private def createRandomZkNode(zkClient: ZkClient): String = {
    val randomPath = s"/kdssmTest-${UUID.randomUUID}"
    logger.trace(s"creating zkPath: $randomPath")
    zkClient.createPersistent(randomPath)
    randomPath
  }
}