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
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.geotools.data.DataStore
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import scala.collection.JavaConverters._

trait KafkaDataStoreSchemaManager extends DataStore  {

  protected def zookeepers: String
  protected def zkPath: String
  protected def partitions: Int
  protected def replication: Int

  //
  // todo:  consistent exception handling across all methods
  // todo   - catch and rethrow a known/consistent exception with helpful msg
  //
   override def createSchema(featureType: SimpleFeatureType): Unit = {

    val kfc = KafkaFeatureConfig(featureType)  // this is guaranteed to have a topic

    // build the schema node
    val typeName = featureType.getTypeName
    val schemaPath: String = getSchemaPath(typeName)
    if (zkClient.exists(schemaPath)) {
      throw new IllegalArgumentException(s"Type $typeName already exists")
    }

    val data = SimpleFeatureTypes.encodeType(featureType)
    createZkNode(schemaPath, data)

    // build the topic node
    createZkNode(getTopicPath(typeName), kfc.topic)

    // build the replay config node (optional)
    // build the topic node
    kfc.replayConfig.foreach(r => createZkNode(getReplayConfigPath(typeName), ReplayConfig.encode(r)))

    //create the Kafka topic
    if(!AdminUtils.topicExists(zkClient, kfc.topic))
      AdminUtils.createTopic(zkClient, kfc.topic, partitions, replication)

    // put it in the cache
    schemaCache.put(typeName, kfc)

  }

  def getFeatureConfig(typeName: String) : KafkaFeatureConfig = schemaCache.get(typeName)

  /** Extracts the prepared-for-live [[SimpleFeatureType]] which the given prepared-for-replay
    * ``replayType`` is based on.
    */
  def getLiveFeatureType(replayType: SimpleFeatureType): Option[SimpleFeatureType] = {

    try {
      KafkaDataStoreHelper.extractLiveTypeName(replayType).map(schemaCache.get(_).sft)
    }
    catch {
      case _ => None
    }
  }

  override def getNames: util.List[Name] = zkClient.getChildren(zkPath).asScala.map(
    name => new NameImpl(name) : Name).asJava


  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  override def removeSchema(typeName: String): Unit = {

    // clean up cache and zookeeper resources
    schemaCache.invalidate(typeName)
    zkClient.deleteRecursive(getSchemaPath(typeName))

    // todo: what about other remote caches - do we set up a kafka listener looking for deletes?
    // todo: what about kafka - who has the responsibility to clean the kafka topic up?

  }

  private def resolveTopicSchema(typeName: String): KafkaFeatureConfig = {

    val schema = zkClient.readData[String](getSchemaPath(typeName)) //throws ZkNoNodeException if not found
    val topic = zkClient.readData[String](getTopicPath(typeName)) // throws Zk...
    val sft = SimpleFeatureTypes.createType(typeName, schema)
    KafkaDataStoreHelper.insertTopic(sft, topic)
    val replay = Option(zkClient.readData[String](getReplayConfigPath(typeName), true))
    replay.map(KafkaDataStoreHelper.insertReplayConfig(sft, _))

    KafkaFeatureConfig(sft)
  }

  def getSchemaPath(typeName: String): String = {
    s"$zkPath/$typeName"
  }
  def getTopicPath(typeName: String): String = {
    s"$zkPath/$typeName/Topic"
  }
  def getReplayConfigPath(typeName: String): String = {
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
        resolveTopicSchema(k)
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


