/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka08

import java.util

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.geotools.data.DataStore
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.kafka.{KafkaDataStoreHelper, ReplayConfig}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.GeoMesaSchemaValidator
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

/** A partial implementation of [[DataStore]] implementing all methods related to [[SimpleFeatureType]]s.
  *
  * See GEOMESA-818 for additional considerations.
  */
trait KafkaDataStoreSchemaManager extends DataStore with LazyLogging {

  protected def zookeepers: String
  protected def zkPath: String
  protected def partitions: Int
  protected def replication: Int

  override def createSchema(featureType: SimpleFeatureType): Unit = {

    val kfc = KafkaFeatureConfig(featureType)  // this is guaranteed to have a topic

    // build the schema node
    val typeName = featureType.getTypeName
    val schemaPath: String = getSchemaPath(typeName)
    if (zkUtils.zkClient.exists(schemaPath)) {
      throw new IllegalArgumentException(s"Type $typeName already exists at $zkPath.")
    }

    // inspect and update the simple feature type for various components
    // do this before anything else so that any modifications will be in place
    GeoMesaSchemaValidator.validate(featureType)


    val data = SimpleFeatureTypes.encodeType(featureType, includeUserData = true)
    createZkNode(schemaPath, data)

    // build the topic node
    createZkNode(getTopicPath(typeName), kfc.topic)

    // build the replay config node (optional)
    // build the topic node
    kfc.replayConfig.foreach(r => createZkNode(getReplayConfigPath(typeName), ReplayConfig.encode(r)))

    //create the Kafka topic
    if (!zkUtils.topicExists(kfc.topic)) {
      zkUtils.createTopic(kfc.topic, partitions, replication)
    }

    // Ensure the topic is ready before adding the new layer to the SFT cache
    waitUntilTopicReady(zkUtils, kfc.topic, partitions)

    // put it in the cache
    schemaCache.put(typeName, kfc)
  }

  // Wait until a topic is ready for writes.
  // NB: The assumption here is that waiting for the leader to be elected is sufficient.
  // Further, this function delegates to a function which waits for a leader for each partition.
  def waitUntilTopicReady(zkUtils: ZkUtils08, topic: String, partitions: Int, timeToWait: Long = 5000L): Unit = {
    (0 until partitions).foreach { waitUntilTopicReadyForPartition(zkUtils, topic, _, timeToWait) }
  }

  def waitUntilTopicReadyForPartition(zkUtils: ZkUtils08, topic: String, partition: Int, timeToWait: Long) {
    var leader: Option[Int] = None
    val start = System.currentTimeMillis

    while(leader.isEmpty && System.currentTimeMillis < start + timeToWait) {
      leader = zkUtils.getLeaderForPartition(topic, partition)
      if (leader.isEmpty) {
        logger.debug(s"Still waiting on a leader for topic: $topic")
        Thread.sleep(100)
      }
    }

    leader match {
      case Some(l) => logger.debug(s"Got a leader for topic: $topic")
      case _       =>
        throw new Exception(s"Failed to get a leader for partition $partition of " +
                            s"topic: $topic within $timeToWait milliseconds.")
    }
  }

  def getFeatureConfig(typeName: String) : KafkaFeatureConfig = schemaCache.get(typeName)

  /** Extracts the "Streaming SFT" which the given "Replay SFT" is based on.
    */
  def getLiveFeatureType(replayType: SimpleFeatureType): Option[SimpleFeatureType] = {

    Try {
      KafkaDataStoreHelper.extractStreamingTypeName(replayType).map(schemaCache.get(_).sft)
    }.getOrElse(None)
  }

  override def getNames: util.List[Name] = {
    zkUtils.zkClient.getChildren(zkPath).asScala
      .filter(name => zkUtils.zkClient.exists(getTopicPath(name)))
      .map(name => new NameImpl(name): Name).asJava
  }

  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  override def removeSchema(typeName: String): Unit = {

    // grab feature config before deleting it
    val fct = Try { getFeatureConfig(typeName) }

    // clean up cache and zookeeper resources
    schemaCache.invalidate(typeName)
    zkUtils.zkClient.deleteRecursive(getSchemaPath(typeName))

    // delete topic for "Streaming SFTs" but not for "Replay SFTs"
    fct.foreach { fc =>
      if (KafkaDataStoreHelper.isStreamingSFT(fc.sft) && zkUtils.topicExists(fc.topic)) {
        zkUtils.deleteTopic(fc.topic)
      }
    }
  }

  def updateKafkaSchema(typeName: String, sft: SimpleFeatureType) = {
    import SimpleFeatureTypes.Configs._
    import SimpleFeatureTypes.InternalConfigs._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    // Get previous schema and user data
    val previousSft = getSchema(typeName)
    val schemaTypeName = sft.getTypeName

    // Prevent modifying wrong type if type names don't match
    if (!schemaTypeName.equals(typeName.toString)) {
      throw new IllegalArgumentException(s"Provided type name $typeName does not match schema type name $schemaTypeName")
    }

    if (previousSft == null) {
      throw new IllegalArgumentException(s"No schema found for given type name $typeName")
    }

    // Check that unmodifiable user data has not changed
    val unmodifiableUserdataKeys = Set(SCHEMA_VERSION_KEY, TABLE_SHARING_KEY, SHARING_PREFIX_KEY,
      DEFAULT_DATE_KEY, ST_INDEX_SCHEMA_KEY, ENABLED_INDICES)

    unmodifiableUserdataKeys.foreach { key =>
      if (sft.getUserData.keySet().contains(key) && sft.userData[String](key) != previousSft.userData[String](key)) {
        throw new UnsupportedOperationException(s"Updating $key is not allowed")
      }
    }

    // Check that the rest of the schema has not changed (columns, types, etc)
    val previousColumns = previousSft.getAttributeDescriptors
    val currentColumns = sft.getAttributeDescriptors
    if (previousColumns != currentColumns) {
      throw new UnsupportedOperationException("Updating schema columns is not allowed")
    }

    // If all is well, write to zookeeper metadata
    zkUtils.zkClient.writeData(getSchemaPath(typeName), SimpleFeatureTypes.encodeType(sft, includeUserData = true))
  }

  override def dispose(): Unit = {
    zkUtils.close()
  }

  private def resolveTopicSchema(typeName: String): KafkaFeatureConfig = {

    val schema = zkUtils.zkClient.readData[String](getSchemaPath(typeName)) //throws ZkNoNodeException if not found
    val topic = zkUtils.zkClient.readData[String](getTopicPath(typeName)) // throws Zk...
    val sft = SimpleFeatureTypes.createType(typeName, schema)
    KafkaDataStoreHelper.insertTopic(sft, topic)
    val replay = Option(zkUtils.zkClient.readData[String](getReplayConfigPath(typeName), true))
    replay.foreach(KafkaDataStoreHelper.insertReplayConfig(sft, _))

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

  private val zkUtils = {
    val ret = KafkaUtils08.createZkUtils(zookeepers, Int.MaxValue, Int.MaxValue)

    // build the top level zookeeper node
    if (!ret.zkClient.exists(zkPath)) {
      try {
        ret.zkClient.createPersistent(zkPath, true)
      } catch {
        case e: ZkNodeExistsException => // it's ok, something else created before we could
        case e: Exception => throw new RuntimeException(s"Could not create path in zookeeper at $zkPath", e)
      }
    }
    ret
  }

  private def createZkNode(path: String, data: String) {
    try {
      zkUtils.zkClient.createPersistent(path, data)
    } catch {
      case e: ZkNodeExistsException =>
        throw new IllegalArgumentException(s"Node $path already exists", e)
      case NonFatal(e) =>
        throw new RuntimeException(s"Could not create path in zookeeper at $path", e)
    }
  }

  private val schemaCache =
    Caffeine.newBuilder().build(new CacheLoader[String, KafkaFeatureConfig] {
      override def load(k: String): KafkaFeatureConfig =
        resolveTopicSchema(k)
    })
}

/** This is an internal configuration class shared between KafkaDataStore and the various feature sources
  * (producer, live consumer, replay consumer).
  *
  * @constructor
  * @param sft the [[SimpleFeatureType]]
  * @throws IllegalArgumentException if ``sft`` has not been prepared by calling
  *                                  ``KafkaDataStoreHelper.prepareForLive``
  */
@throws[IllegalArgumentException]
private[kafka08] case class KafkaFeatureConfig(sft: SimpleFeatureType) {

  /** the name of the Kafka topic */
  val topic: String = KafkaDataStoreHelper.extractTopic(sft)
    .getOrElse(throw new IllegalArgumentException(
    s"The SimpleFeatureType '${sft.getTypeName}' may not be used with KafkaDataStore because it has "
      + "not been 'prepared' via KafkaDataStoreHelper.prepareForLive"))


  /** the [[ReplayConfig]], if any */
  val replayConfig: Option[ReplayConfig] = KafkaDataStoreHelper.extractReplayConfig(sft)

  override def toString: String =
    s"KafkaFeatureConfig: typeName=${sft.getTypeName}; topic=$topic; replayConfig=$replayConfig"
}
