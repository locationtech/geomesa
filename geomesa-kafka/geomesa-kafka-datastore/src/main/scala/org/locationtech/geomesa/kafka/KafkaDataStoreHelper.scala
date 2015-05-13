package org.locationtech.geomesa.kafka

import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.opengis.feature.simple.SimpleFeatureType

class KafkaDataStoreHelper {

  private val TopicKey = "Topic"
  private val ReplayConfigKey = "ReplayConfig"

  def prepareForLive(sft: SimpleFeatureType, zkPath: String) : SimpleFeatureType = {

    val topic = buildTopicName(zkPath, sft)
    val sftCopy = copySft(sft)
    sftCopy.getUserData.put(TopicKey,topic)

    sftCopy
  }

  def prepareForReplay(sft: SimpleFeatureType, rConfig: ReplayConfig) : SimpleFeatureType = {
    val rcString = ReplayConfig.encode(rConfig);
    val sftCopy = copySft(sft);
    sftCopy.getUserData.put(ReplayConfigKey,rcString);

    sftCopy
  }

  def extractTopic(sft: SimpleFeatureType) : Option[String] =
    Option(sft.getUserData.get(TopicKey)).map(_.asInstanceOf[String])


  def extractReplayConfig(sft: SimpleFeatureType) : Option[ReplayConfig] = {
    // this one is pretty cool, but not very readable :)
    Option(sft.getUserData.get(ReplayConfigKey)).map(_.asInstanceOf[String]).map(ReplayConfig.decode(_).get)
  }

  private def buildTopicName(zkPath: String, sft: SimpleFeatureType): String = {
    return sft.getTypeName + "_" + zkPath;
  }

  private def copySft(sft: SimpleFeatureType): SimpleFeatureType = {

    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    val preppedSft: SimpleFeatureType = builder.buildFeatureType()

    // builder doesn't copy user data...
    preppedSft.getUserData.putAll(sft.getUserData)

    preppedSft
  }
}
