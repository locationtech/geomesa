package org.locationtech.geomesa.core.data

import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding

object SimpleFeatureEncoderFactory {

  def createEncoder(featureEncoding: String): SimpleFeatureEncoder =
    createEncoder(FeatureEncoding.withName(featureEncoding))

  def defaultEncoder = new AvroFeatureEncoder

  def createEncoder(featureEncoding: FeatureEncoding): SimpleFeatureEncoder =
    featureEncoding match {
      case FeatureEncoding.AVRO => new AvroFeatureEncoder
      case FeatureEncoding.TEXT => new TextFeatureEncoder
    }
}
