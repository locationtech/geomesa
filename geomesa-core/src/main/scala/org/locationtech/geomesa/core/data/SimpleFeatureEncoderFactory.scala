package org.locationtech.geomesa.core.data

import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.opengis.feature.simple.SimpleFeatureType

object SimpleFeatureEncoderFactory {

  def createEncoder(sft: SimpleFeatureType, str: String): SimpleFeatureEncoder =
    createEncoder(sft, encoding = FeatureEncoding.withName(str).asInstanceOf[FeatureEncoding])

  def defaultEncoder(sft: SimpleFeatureType) = createEncoder(sft, FeatureEncoding.AVRO)

  def createEncoder(sft: SimpleFeatureType, encoding: FeatureEncoding): SimpleFeatureEncoder =
    encoding match {
      case FeatureEncoding.AVRO => new AvroFeatureEncoder(sft)
      case FeatureEncoding.TEXT => new TextFeatureEncoder(sft)
    }
}
