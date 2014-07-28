package geomesa.feature

import geomesa.utils.geotools.SimpleFeatureTypes
import org.geotools.data.DataUtilities
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.GeometryBuilder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureFactoryTest extends Specification {

  "GeoTools must use AvroSimpleFeatureFactory when hint is set" in {
    AvroSimpleFeatureFactory.init

    val featureFactory = CommonFactoryFinder.getFeatureFactory(null)
    featureFactory.getClass mustEqual classOf[AvroSimpleFeatureFactory]
  }


  "SimpleFeatureBuilder should return an AvroSimpleFeature when using an AvroSimpleFeatureFactory" in {
    AvroSimpleFeatureFactory.init
    val geomBuilder = new GeometryBuilder(DefaultGeographicCRS.WGS84)
    val featureFactory = CommonFactoryFinder.getFeatureFactory(null)
    val sft = SimpleFeatureTypes.createType("testavro", "name:String,geom:Point:srid=4326")
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    builder.reset()
    builder.add("Hello")
    builder.add(geomBuilder.createPoint(1,1))
    val feature = builder.buildFeature("id")

    feature.getClass mustEqual classOf[AvroSimpleFeature]
  }

}
