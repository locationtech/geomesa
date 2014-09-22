package org.locationtech.geomesa.core.transform

import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.core.data.{FeatureEncoding, SimpleFeatureEncoder}
import org.locationtech.geomesa.feature.{AvroSimpleFeature, AvroSimpleFeatureFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object TransformCreator {

  /**
   * Create a function to transform a feature from one sft to another...this will
   * result in a new feature instance being created and encoded.
   *
   * The function returned may NOT be ThreadSafe to due the fact it contains a
   * SimpleFeatureEncoder instance which is not thread safe to optimize performance
   */
  def createTransform(targetFeatureType: SimpleFeatureType,
                      featureEncoding: FeatureEncoding,
                      transformString: String): (SimpleFeature => Array[Byte]) =
    featureEncoding match {
      case FeatureEncoding.AVRO =>
        val encoder = SimpleFeatureEncoder(targetFeatureType, featureEncoding)
        val defs = TransformProcess.toDefinition(transformString)
        (feature: SimpleFeature) => {
          val newSf = new AvroSimpleFeature(feature.getIdentifier, targetFeatureType)
          defs.foreach { t => newSf.setAttribute(t.name, t.expression.evaluate(feature)) }
          encoder.encode(newSf)
        }

      case FeatureEncoding.TEXT =>
        val defs = TransformProcess.toDefinition(transformString)
        val encoder = SimpleFeatureEncoder(targetFeatureType, featureEncoding)
        val builder = AvroSimpleFeatureFactory.featureBuilder(targetFeatureType)
        (feature: SimpleFeature) => {
          builder.reset()
          defs.foreach { t => builder.set(t.name, t.expression.evaluate(feature)) }
          val newFeature = builder.buildFeature(feature.getID)
          encoder.encode(newFeature)
        }
    }

}
