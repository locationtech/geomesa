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

import java.util

import com.google.common.cache.{CacheLoader, CacheBuilder}
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.geotools.data.DataStore
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import scala.collection.JavaConverters._
//import scala.collection.JavaConversions._

trait KafkaDataStoreSchemaManager extends DataStore  {


  protected def zookeepers: String
  protected def zkPath: String
  protected def partitions: Int
  protected def replication: Int

  override def createSchema(featureType: SimpleFeatureType): Unit = {

    val kfc = KafkaFeatureConfig(featureType)  // this is guaranteed to have a topic

    // build the schema node
    val typeName = featureType.getTypeName
    val schemaPath: String = getSchemaPath(typeName)
    if (zkClient.exists(schemaPath)) {  // todo:  look into zk exceptions
      throw new IllegalArgumentException(s"Type $typeName already exists")
    }

    val data = SimpleFeatureTypes.encodeType(featureType)
    createZkNode(schemaPath, data)

    // build the topic node
    createZkNode(getTopicPath(typeName), kfc.topic)

    // build the replay config node (optional)
    // build the topic node
    val replayPath = getSchemaPath(typeName)
    kfc.replayConfig.foreach(r => createZkNode(getReplayConfigPath(typeName), ReplayConfig.encode(r))) // todo:  look into zk exceptions

    //create the Kafka topic
    AdminUtils.createTopic(zkClient, kfc.topic, partitions, replication)  // todo:  look into zk exceptions

    // put it in the cache
    schemaCache.put(typeName, kfc)

  }

  def getFeatureConfig(typeName: String) : KafkaFeatureConfig = schemaCache.get(typeName)

  override def getNames: util.List[Name] = {

    zkClient.getChildren(zkPath).asScala.map(name => new NameImpl(name) : Name).asJava

  }

  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  override def removeSchema(typeName: String): Unit = {
    schemaCache.invalidate(typeName)
    ???  // todo: worry about zook, don't worry about topic.  what about other remote caches - do we set up a kafka
         // listener looking for deletes? */
  }

  // todo: need to grab rc and topic from zk.  Also consider not returning an option
  private def resolveTopicSchema(typeName: String): Option[KafkaFeatureConfig] = {

    val schema = zkClient.readData[String](getSchemaPath(typeName))  //throws ZkNoNodeException if not found
    val topic = zkClient.readData[String](getTopicPath(typeName))    // throws Zk...
    val sft = SimpleFeatureTypes.createType(typeName, schema)
    KafkaDataStoreHelper.insertTopic(sft, topic)
    val replay = Option(zkClient.readData[String](getReplayConfigPath(typeName))) // throws Zk...
    replay.map(KafkaDataStoreHelper.insertReplayConfig(sft,_))

    Option(KafkaFeatureConfig(sft)) // does this really need to be an option?
  }

  private def getSchemaPath(typeName: String): String = {
    s"$zkPath/$typeName"
  }
  private def getTopicPath(typeName: String): String = {
    s"$zkPath/$typeName/Topic"
  }
  private def getReplayConfigPath(typeName: String): String = {
    s"$zkPath/$typeName/ReplayConfig"
  }

  private val zkClient = {
    // zkStringSerializer is required - otherwise topics won't be created correctly
    val ret = new ZkClient(zookeepers, Int.MaxValue, Int.MaxValue, ZKStringSerializer)

    // build the top level zookeeper node
    if (!ret.exists(zkPath)) {
      try {
        ret.createPersistent(zkPath, true)
      } catch {
        case e: ZkNodeExistsException => // it's ok, something else created before we could
        case e: Exception => throw new RuntimeException(s"Could not create path in zookeeper at $zkPath", e)
      }
    }
    ret
  }

  private def createZkNode(path: String, data: String) {
    try {
      zkClient.createPersistent(path, data)
    } catch {
      case e: ZkNodeExistsException =>
        throw new IllegalArgumentException(s"Node $path already exists", e)
      case e: Exception =>
        throw new RuntimeException(s"Could not create path in zookeeper at $path", e)
    }
  }

  private val schemaCache =
    CacheBuilder.newBuilder().build(new CacheLoader[String, KafkaFeatureConfig] {
      override def load(k: String): KafkaFeatureConfig =
        resolveTopicSchema(k).getOrElse(throw new IllegalArgumentException(s"Unable to find schema with name $k"))
    })
}

/** This is an internal configuration class shared between KafkaDataStore and the various feature sources
  * (producer, live consumer, replay consumer).
  *
  * @constructor
  *
  * @param sft the [[SimpleFeatureType]]
  * @throws IllegalArgumentException if ``sft`` has not been prepared by calling
  *                                  ``KafkaDataStoreHelper.prepareForLive``
  */
@throws[IllegalArgumentException]
private[kafka] case class KafkaFeatureConfig(sft: SimpleFeatureType) extends AnyRef {

  /** the name of the Kafka topic */
  val topic: String = KafkaDataStoreHelper.extractTopic(sft)
    .getOrElse(throw new IllegalArgumentException(
    s"The SimpleFeatureType '${sft.getTypeName}' may not be used with KafkaDataStore because it has "
      + "not been 'prepared' via KafkaDataStoreHelper.prepareForLive"))


  /** the [[ReplayConfig]], if any */
  val replayConfig: Option[ReplayConfig] = KafkaDataStoreHelper.extractReplayConfig(sft)

  override def toString: String =
    s"KafkaSimpleFeatureType: typeName=${sft.getTypeName}; topic=$topic; replayConfig=$replayConfig"
}
