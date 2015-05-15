package org.locationtech.geomesa.kafka

import java.util.UUID

import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureType

object KafkaDataStoreHelper {

  val TopicKey = "Topic"
  val ReplayConfigKey = "ReplayConfig"

  /** Creates a copy of the passed SimpleFeatureType, inserting the topic name (derived from zkPath) into user data.*/
  def prepareForLive(sft: SimpleFeatureType, zkPath: String) : SimpleFeatureType = {

    val builder = FeatureUtils.builder(sft)
    val preparedSft = builder.buildFeatureType()
    insertTopic(preparedSft,buildTopicName(zkPath,sft))
    preparedSft
  }


  /** Creates a copy of the passed SimpleFeature type, inserting an encoded ReplayConfig into the user data. */
  def prepareForReplay(sft: SimpleFeatureType, rConfig: ReplayConfig) : SimpleFeatureType = {

    val builder = FeatureUtils.builder(sft)
    builder.setName(buildReplayTypeName(sft.getTypeName))

    // assumes topic has been prepared, need to check though...
    val preparedSft = builder.buildFeatureType()
    preparedSft.getUserData.put(ReplayConfigKey,ReplayConfig.encode(rConfig))
    preparedSft
  }

  /** modifies the passed SimpleFeature type, adding the topic string to user data */
  def insertTopic(sft: SimpleFeatureType, topic: String) : SimpleFeatureType = {
    sft.getUserData.put(TopicKey,topic)
    sft
  }

  def extractTopic(sft: SimpleFeatureType) : Option[String] = sft.userData[String](TopicKey)

  /** Modifies the passed SimpleFeatureType, adding the encoded replay configString to the user data */
  def insertReplayConfig(sft: SimpleFeatureType, configString : String): SimpleFeatureType = {
    sft.getUserData.put(ReplayConfigKey, configString)
    sft
  }

  def extractReplayConfig(sft: SimpleFeatureType) : Option[ReplayConfig] =
    sft.userData[String](ReplayConfigKey).flatMap(ReplayConfig.decode)

  private[kafka] def buildTopicName(zkPath: String, sft: SimpleFeatureType): String = {
    sft.getTypeName + "_" + zkPath.replaceAll("/","-")  //kafka doesn't like slashes
  }

  private def buildReplayTypeName(name: String): String = {
    val uuid=UUID.randomUUID()
    s"$name-REPLAY-$uuid"
  }

}
