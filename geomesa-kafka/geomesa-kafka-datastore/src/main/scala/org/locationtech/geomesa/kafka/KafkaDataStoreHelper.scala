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
    ReplayTimeHelper.addReplayTimeAttribute(builder)

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

  /** Extracts the name of the prepared--for-live [[SimpleFeatureType]] which the given prepared-for-replay
    * ``replayType`` is based on.
    *
    * @param replayType a [[SimpleFeatureType]] that has been prepared for replay
    * @return the name of the live simple feature or ``None`` if ``replayType`` was not prepared for replay
    */
  def extractLiveTypeName(replayType: SimpleFeatureType): Option[String] = {
    val replayName = replayType.getTypeName
    val index = replayName.indexOf(replayIdentifier)

    if (index > 0) {
      Some(replayName.substring(0, index))
    } else {
      None
    }
  }
  
  def isPreparedForLive(sft: SimpleFeatureType): Boolean =
    sft.getUserData.containsKey(TopicKey) && !sft.getUserData.containsKey(ReplayConfigKey)

  def isPreparedForReplay(sft: SimpleFeatureType): Boolean = sft.getUserData.containsKey(ReplayConfigKey)

  final val DefaultZkPath: String = "/geomesa/ds/kafka"

  /** Cleans up a zk path parameter - trims, prepends with "/" if needed, strips trailing "/" if needed.
    * Defaults to "/geomesa/ds/kafka" if rawPath is null or empty.
    * @param rawPath
    * @return
    */
  def cleanZkPath(rawPath: String, default: String = DefaultZkPath): String = {
    Option(rawPath).map(_.trim)  // handle null
      .filterNot(_.isEmpty)  // handle empty string
      .map(p => if (p.startsWith("/")) p else "/" + p)  // leading '/'
      .map(p => if (p.endsWith("/") && (p.length > 1)) p.substring(0, p.length - 1) else p)  // trailing '/'
      .getOrElse(default) // default case
  }

  private[kafka] def buildTopicName(zkPath: String, sft: SimpleFeatureType): String = {
    sft.getTypeName + "_" + zkPath.replaceAll("/","-")  //kafka doesn't like slashes
    zkPath.replaceAll("/","-") + "-"+  sft.getTypeName
  }

  private val replayIdentifier = "-REPLAY-"

  private def buildReplayTypeName(name: String): String = {
    val uuid=UUID.randomUUID()
    s"$name$replayIdentifier$uuid"
  }

}
