package geomesa.feature

import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.AbstractFeatureFactoryImpl
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class AvroSimpleFeatureFactory extends AbstractFeatureFactoryImpl {

  override def createSimpleFeature(attrs: Array[AnyRef],
                                   sft: SimpleFeatureType,
                                   id: String): SimpleFeature = {
    val f = new AvroSimpleFeature(new FeatureIdImpl(id), sft)
    f.setAttributes(attrs)
    f
  }

  override def createSimpleFeautre(attrs: Array[AnyRef],
                                   descriptor: AttributeDescriptor,
                                   id: String): SimpleFeature =
    createSimpleFeature(attrs, descriptor.asInstanceOf[SimpleFeatureType], id)

}

object AvroSimpleFeatureFactory {
  def init = {
    Hints.putSystemDefault(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
  }

  private val hints = new Hints(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
  private val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)

  def buildAvroFeature(sft: SimpleFeatureType, attrs: Seq[AnyRef], id: String) = {
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    builder.addAll(attrs)
    builder.buildFeature(id)
  }

  def featureBuilder(sft: SimpleFeatureType) = new SimpleFeatureBuilder(sft, featureFactory)

}
