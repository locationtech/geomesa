package geomesa.core.transform

import collection.JavaConversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import geomesa.core.data.{FeatureEncoding, SimpleFeatureEncoder}
import org.geotools.process.vector.TransformProcess
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.apache.accumulo.core.data.Value
import geomesa.avro.scala.AvroSimpleFeature

object TransformCreator {
  def createTransform(targetFeatureType: SimpleFeatureType,
                      featureEncoder: SimpleFeatureEncoder,
                      transformString: String): (SimpleFeature => Value) = (featureEncoder.getName) match {
    case n if FeatureEncoding.AVRO.toString.equals(n) =>
      val defs = TransformProcess.toDefinition(transformString)
      (feature: SimpleFeature) => {
        val newSf = new AvroSimpleFeature(feature.getIdentifier, targetFeatureType)
        defs.map { t => newSf.setAttribute(t.name, t.expression.evaluate(feature)) }
        new Value(featureEncoder.encode(newSf))
      }

    case n if FeatureEncoding.TEXT.toString.equals(n) =>
      val defs = TransformProcess.toDefinition(transformString)
      val builder = new SimpleFeatureBuilder(targetFeatureType)
      (feature: SimpleFeature) => {
        builder.reset()
        defs.map { t => builder.set(t.name, t.expression.evaluate(feature)) }
        val newFeature = builder.buildFeature(feature.getID)
        new Value(featureEncoder.encode(newFeature))
      }
  }
}
