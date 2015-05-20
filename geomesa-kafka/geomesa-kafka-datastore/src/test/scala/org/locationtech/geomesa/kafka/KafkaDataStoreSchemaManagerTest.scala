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

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.geotools.data.store.{ContentDataStore, ContentEntry}
import org.geotools.feature.NameImpl
import org.joda.time.{Duration, Instant}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._
import scala.util.Try

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreSchemaManagerTest
  extends Specification
  with Logging
  with HasEmbeddedZookeeper {

  // todo: missing tests -
  // todo    test general exception handling (use zk mock for this?)

  "create without being prepped should fail " >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-create-no-prep"
      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      Try(datastore.createSchema(sft)) must beFailedTry
    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }

  "create prepped for Replay without being prepped Live should fail " >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-create-replay-no-prep"
      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))

      Try(datastore.createSchema(KafkaDataStoreHelper.prepareForReplay(sft,rc))) must beFailedTry
    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }

  "create prepped live should have exactly one schema node and exactly one topic node" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-create-live"
      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      datastore.createSchema(KafkaDataStoreHelper.prepareForLive(sft, zkPath))

      zkClient.countChildren(zkPath) mustEqual 1

      val zChildren = zkClient.getChildren(zkPath)
      zChildren.size() mustEqual 1

      zChildren.get(0) mustEqual typename
      val sPath = datastore.getSchemaPath(typename)
      val encoded = zkClient.readData[String](sPath)
      SimpleFeatureTypes.createType(typename, encoded)

      val sChildren = zkClient.getChildren(sPath)
      sChildren.size() mustEqual 1
      sChildren.get(0) mustEqual "Topic"// just check no exceptions

      val topic = zkClient.readData[String](datastore.getTopicPath(typename))
      AdminUtils.topicExists(zkClient,topic) must beTrue
    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }

  "create should fail if type already created" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-create-live-already-exists"
      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      datastore.createSchema(KafkaDataStoreHelper.prepareForLive(sft, zkPath))

      val sft2 = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      Try(datastore.createSchema(KafkaDataStoreHelper.prepareForLive(sft2, zkPath))) must beFailedTry

    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }
  "create prepped replay should have exactly one schema node and exactly one topic node and one replay node" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)
    try {

      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-create-replay"
      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))

      datastore.createSchema(KafkaDataStoreHelper.prepareForReplay(
        KafkaDataStoreHelper.prepareForLive(sft,zkPath), rc))

      zkClient.countChildren(zkPath) mustEqual 1

      val zChildren = zkClient.getChildren(zkPath)
      zChildren.size() mustEqual 1

      val replayTypename = zChildren.get(0)
      replayTypename must contain(typename) and contain("REPLAY")

      val sPath = datastore.getSchemaPath(replayTypename)
      val encoded = zkClient.readData[String](sPath)
      SimpleFeatureTypes.createType(typename, encoded) // just check no exceptions

      val sChildren = zkClient.getChildren(sPath).asScala
      sChildren.size mustEqual 2
      sChildren must contain("Topic","ReplayConfig")

      val topic = zkClient.readData[String](datastore.getTopicPath(replayTypename))
      AdminUtils.topicExists(zkClient,topic) must beTrue
    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }

  "getFeatureConfig where type doesn't exists throws exception " >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typeName = "test-get-doesNotExist"

      Try(datastore.getFeatureConfig(typeName)) must beFailedTry
    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }

  }

  "getFeatureConfig can retrieve an existing schema" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-get"
      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))

      val prepped = KafkaDataStoreHelper.prepareForReplay(
        KafkaDataStoreHelper.prepareForLive(sft, zkPath), rc)

      datastore.createSchema(prepped)

      val fct = Try(datastore.getFeatureConfig(prepped.getTypeName))
      fct must beSuccessfulTry
      KafkaDataStoreHelper.extractTopic(prepped) must beSome(fct.get.topic)
      val fc = fct.get
      fc.replayConfig must beSome(rc)
      fc.sft mustEqual prepped
      fc.sft.getUserData mustEqual prepped.getUserData
    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }

  }

  "getNames returns all created types" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-getnames"

      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val livePrepped = KafkaDataStoreHelper.prepareForLive(sft,zkPath)
      datastore.createSchema(livePrepped)

      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val replayPrepped = KafkaDataStoreHelper.prepareForReplay(livePrepped,rc)
      datastore.createSchema(replayPrepped)

      val names = datastore.getNames().asScala
      val expected = List(livePrepped.getTypeName,replayPrepped.getTypeName).
        map(name => new NameImpl(name):Name)

      names must containTheSameElementsAs(expected)

    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }

  "removeSchema (string) should remove the specified schema" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {

      //create two schemas (one replay, one live)
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-remove-string"

      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val livePrepped = KafkaDataStoreHelper.prepareForLive(sft,zkPath)
      datastore.createSchema(livePrepped)

      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val replayPrepped = KafkaDataStoreHelper.prepareForReplay(livePrepped,rc)
      datastore.createSchema(replayPrepped)

      // verify there are two
      datastore.getNames.size mustEqual 2

      // now remove the replay
      datastore.removeSchema(replayPrepped.getTypeName)

      datastore.getNames.size mustEqual 1
      Try(datastore.getFeatureConfig(replayPrepped.getTypeName)) must beAFailedTry
      Try(datastore.getFeatureConfig(livePrepped.getTypeName)) must beASuccessfulTry

      zkClient.exists(datastore.getSchemaPath(replayPrepped.getTypeName)) must beFalse
      zkClient.exists(datastore.getSchemaPath(livePrepped.getTypeName)) must beTrue

    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }

  "removeSchema (Name) should remove the specified schema" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {

      //create two schemas (one replay, one live)
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-remove-name"

      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val livePrepped = KafkaDataStoreHelper.prepareForLive(sft,zkPath)
      datastore.createSchema(livePrepped)

      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val replayPrepped = KafkaDataStoreHelper.prepareForReplay(livePrepped,rc)
      datastore.createSchema(replayPrepped)

      // verify there are two
      datastore.getNames.size mustEqual 2

      // now remove the replay
      datastore.removeSchema(new NameImpl(replayPrepped.getTypeName))

      datastore.getNames.size mustEqual 1
      Try(datastore.getFeatureConfig(replayPrepped.getTypeName)) must beAFailedTry
      Try(datastore.getFeatureConfig(livePrepped.getTypeName)) must beASuccessfulTry

      zkClient.exists(datastore.getSchemaPath(replayPrepped.getTypeName)) must beFalse
      zkClient.exists(datastore.getSchemaPath(livePrepped.getTypeName)) must beTrue

    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }

  "getLiveFeatureType should retrieve the appropriate original schema" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {

      //create two schemas (one replay, one live)
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-getlive"

      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val livePrepped = KafkaDataStoreHelper.prepareForLive(sft,zkPath)
      datastore.createSchema(livePrepped)

      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val replayPrepped = KafkaDataStoreHelper.prepareForReplay(livePrepped,rc)
      datastore.createSchema(replayPrepped)

      val origSft = datastore.getLiveFeatureType(replayPrepped).
        getOrElse(throw new RuntimeException(s"Failed to find original type for ${replayPrepped.getTypeName}"))
      origSft mustEqual livePrepped
      origSft.getUserData mustEqual livePrepped.getUserData

    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }

  "getLiveFeatureType should return None if the original schema doesn't exist" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {

      //create two schemas (one replay, one live)
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-getlive-noexists"

      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val livePrepped = KafkaDataStoreHelper.prepareForLive(sft,zkPath)
      datastore.createSchema(livePrepped)

      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val replayPrepped = KafkaDataStoreHelper.prepareForReplay(livePrepped,rc)
      datastore.createSchema(replayPrepped)

      datastore.removeSchema(livePrepped.getTypeName)
      datastore.getLiveFeatureType(replayPrepped) must beNone

    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }

  "getLiveFeatureType should return None if not a replay schema" >> {

    val zkClient = new ZkClient(zkConnect, Int.MaxValue, Int.MaxValue, ZKStringSerializer)
    val zkPath = createRandomZkNode(zkClient)

    try {

      //create two schemas (one replay, one live)
      val datastore = new TestDataStore(zkConnect, zkPath)
      val typename = "test-test-live-notreplay"

      val sft = SimpleFeatureTypes.createType(typename, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val livePrepped = KafkaDataStoreHelper.prepareForLive(sft,zkPath)
      datastore.createSchema(livePrepped)

      datastore.getLiveFeatureType(livePrepped) must beNone

    }
    finally {
      logger.trace(s"cleaning up $zkPath")
      zkClient.deleteRecursive(zkPath)
    }
  }
  private def createRandomZkNode(zkClient: ZkClient): String = {
    val randomPath = s"/kdssmTest-${UUID.randomUUID}"
    logger.trace(s"creating zkPath: $randomPath")
    zkClient.createPersistent(randomPath)
    randomPath
  }
}

class TestDataStore(override val zookeepers: String,
                    override val zkPath: String) extends ContentDataStore with KafkaDataStoreSchemaManager with Logging {

  override val partitions: Int = 1
  override val replication: Int = 1

  override def createFeatureSource(entry: ContentEntry) = {
    throw new UnsupportedOperationException(
      "called TestDataStore.createFeatureSource() - this should not have happened!!!")
  }

  override def createTypeNames() = getNames()
}

