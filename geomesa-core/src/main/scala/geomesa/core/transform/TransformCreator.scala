package geomesa.core.transform

import collection.JavaConversions._
import geomesa.core.avro.AvroSimpleFeature
import geomesa.core.data.{FeatureEncoding, SimpleFeatureEncoder}
import org.apache.accumulo.core.data.Value
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.process.vector.TransformProcess
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object TransformCreator {

  def createTransform(targetFeatureType: SimpleFeatureType,
                      featureEncoder: SimpleFeatureEncoder,
                      transformString: String): (SimpleFeature => Value) =
    FeatureEncoding.withName(featureEncoder.getName) match {
      case FeatureEncoding.AVRO =>
        val defs = TransformProcess.toDefinition(transformString)
        (feature: SimpleFeature) => {
          val newSf = new AvroSimpleFeature(feature.getIdentifier, targetFeatureType)
          defs.map { t => newSf.setAttribute(t.name, t.expression.evaluate(feature)) }
          new Value(featureEncoder.encode(newSf))
        }

      case FeatureEncoding.TEXT =>
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
