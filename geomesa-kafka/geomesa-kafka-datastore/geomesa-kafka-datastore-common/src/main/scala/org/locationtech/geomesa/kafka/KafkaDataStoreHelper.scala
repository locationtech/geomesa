/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.util.UUID

import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

/** Utilities for managing the user data in [[SimpleFeatureType]]s as required by [[KafkaDataStore]]
  * 
  */
object KafkaDataStoreHelper {

  val TopicKey = "Topic"
  val ReplayConfigKey = "ReplayConfig"

  /** Creates a copy of the passed [[SimpleFeatureType]] with additional user data specifying the topic name
    * (derived from the given `zkPath`).  The [[KafkaDataStore]] requires the additional user data.  Only
    * "Streaming SFTs" returned by this method and "Replay SFTs" created by `createReplaySFT` may
    * be used with [[KafkaDataStore]].  Calling `createSchema(sft)` on a [[KafkaDataStore]] with any other
    * SFT will result in an [[IllegalArgumentException]] being thrown.
    *
    * @param sft the [[SimpleFeatureType]] to be used with a [[KafkaDataStore]]
    * @param zkPath the base zookeeper path where [[SimpleFeatureType]]s are stored; MUST match the zkPath
    *               used to create the [[KafkaDataStore]]; also used to generate the name of the Kafka topic
    * @return a copy of the given ``sft`` that is ready to be used with a [[KafkaDataStore]]
    */
  def createStreamingSFT(sft: SimpleFeatureType, zkPath: String) : SimpleFeatureType = {
    val streamingSft = FeatureUtils.builder(sft).buildFeatureType()
    insertTopic(streamingSft, buildTopicName(zkPath, sft))
    streamingSft
  }

  /** Creates a copy of the passed SimpleFeature type with additional user data representing the given
    * [[ReplayConfig]] (encoded as a [[String]]).  The [[KafkaDataStore]] requires the additional user data.
    * Only "Streaming SFTs" returned by `createStreamingSFT` and "Replay SFTs" created by this method may
    * be used with [[KafkaDataStore]].  Calling `createSchema(sft)` on a [[KafkaDataStore]] with any other
    * SFT will result in an [[IllegalArgumentException]] being thrown.
    *
    * @param sft the [[SimpleFeatureType]] to prepare for replay; must have been previously prepared for live
    *
    */
  def createReplaySFT(sft: SimpleFeatureType, rConfig: ReplayConfig) : SimpleFeatureType = {
    require(isStreamingSFT(sft),
      "Only \"Streaming SFTs\" created by 'createStreamingSFT' can be used with 'createReplaySFT'.")

    val builder = FeatureUtils.builder(sft)
    builder.setName(buildReplayTypeName(sft.getTypeName))
    ReplayTimeHelper.addReplayTimeAttribute(builder)

    val replaySft = builder.buildFeatureType()
    replaySft.getUserData.put(ReplayConfigKey, ReplayConfig.encode(rConfig))
    replaySft
  }

  /** modifies the passed SimpleFeature type, adding the topic string to user data */
  def insertTopic(sft: SimpleFeatureType, topic: String): Unit =
    sft.getUserData.put(TopicKey,topic)

  def extractTopic(sft: SimpleFeatureType) : Option[String] = sft.userData[String](TopicKey)

  /** Modifies the passed SimpleFeatureType, adding the encoded replay configString to the user data */
  def insertReplayConfig(sft: SimpleFeatureType, configString : String): Unit =
    sft.getUserData.put(ReplayConfigKey, configString)

  def extractReplayConfig(sft: SimpleFeatureType) : Option[ReplayConfig] =
    sft.userData[String](ReplayConfigKey).flatMap(ReplayConfig.decode)

  /** Extracts the name of the "Streaming SFT" [[SimpleFeatureType]] which the given "Replay SFT" is based on.
    *
    * @param replaySFT a [[SimpleFeatureType]] that has been prepared for replay
    * @return the name of the streaming simple feature or ``None`` if ``replaySFT`` was not produced by
    *         'createReplaySFT'
    */
  def extractStreamingTypeName(replaySFT: SimpleFeatureType): Option[String] = {
    val replayName = replaySFT.getTypeName
    val index = replayName.indexOf(replayIdentifier)

    if (index > 0) {
      Some(replayName.substring(0, index))
    } else {
      None
    }
  }

  /** @return true if the given sft was produced by createStreamingSFT otherwise false
    */
  def isStreamingSFT(sft: SimpleFeatureType): Boolean =
    sft.getUserData.containsKey(TopicKey) && !sft.getUserData.containsKey(ReplayConfigKey)

  /** @return true if the given sft was produced by createReplaySFT otherwise false
    */
  def isPreparedForReplay(sft: SimpleFeatureType): Boolean =
    sft.getUserData.containsKey(TopicKey) && sft.getUserData.containsKey(ReplayConfigKey)

  val DefaultZkPath: String = "/geomesa/ds/kafka"

  /** Cleans up a zk path parameter - trims, prepends with "/" if needed, strips trailing "/" if needed.
    * Defaults to "/geomesa/ds/kafka" if rawPath is null or empty.
    *
    * @param rawPath the path to be cleaned
    * @return
    */
  def cleanZkPath(rawPath: String, default: String = DefaultZkPath): String = {
    Option(rawPath)
      .map(_.trim)  // handle null
      .filterNot(_.isEmpty)  // handle empty string
      .map(p => if (p.startsWith("/")) p else "/" + p)  // leading '/'
      .map(p => if (p.endsWith("/") && (p.length > 1)) p.substring(0, p.length - 1) else p)  // trailing '/'
      .getOrElse(default) // default case
  }

  def buildTopicName(zkPath: String, sft: SimpleFeatureType): String = {
    //kafka doesn't like slashes in topic names
    val prefix = (if (zkPath.startsWith("/")) zkPath.substring(1) else zkPath).replaceAll("/","-")
    prefix + "-" + sft.getTypeName
  }

  private val replayIdentifier = "-REPLAY-"

  private def buildReplayTypeName(name: String): String = {
    val uuid = UUID.randomUUID()
    s"$name$replayIdentifier$uuid"
  }

}
