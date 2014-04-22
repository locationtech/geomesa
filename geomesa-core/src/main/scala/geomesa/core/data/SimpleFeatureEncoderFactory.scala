package geomesa.core.data

import geomesa.core.data.FeatureEncoding.FeatureEncoding

object SimpleFeatureEncoderFactory {

  def createEncoder(featureEncoding: String): SimpleFeatureEncoder = featureEncoding match {
    case "avro" => createEncoder(FeatureEncoding.AVRO)
    case "text" => createEncoder(FeatureEncoding.TEXT)
    case _ => throw new IllegalArgumentException(s"Unsupported feature encoding: $featureEncoding")
  }

  def defaultEncoder = new AvroFeatureEncoder

  def createEncoder(featureEncoding: FeatureEncoding): SimpleFeatureEncoder = featureEncoding match {
    case FeatureEncoding.AVRO => new AvroFeatureEncoder
    case FeatureEncoding.TEXT => new TextFeatureEncoder
  }
}
