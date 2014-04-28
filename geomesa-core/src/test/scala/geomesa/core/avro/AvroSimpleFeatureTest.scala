package geomesa.core.avro

import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureTest extends Specification {

  "AvroSimpleFeature" should {
    "properly convert attributes that are set as strings" in {
      val sft = DataUtilities.createType("testType", "a:Integer,b:Date,*geom:Point:srid=4326")

      val f = new AvroSimpleFeature(new FeatureIdImpl("fakeid"), sft)
      f.setAttribute(0,"1")
      f.setAttribute(1,"2013-01-02T00:00:00.000Z")
      f.setAttribute(2,"POINT(45.0 49.0)")

      f.getAttribute(0) must beAnInstanceOf[java.lang.Integer]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }
  }
}
