package geomesa.core.util

import org.specs2.mutable.Specification
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.opengis.feature.simple.SimpleFeatureType
import org.geotools.data.DataUtilities

@RunWith(classOf[JUnitRunner])
class MultiCollectionTest  extends Specification {

  "MultiCollection" should {
    "return return the proper schema" in {
      val multi = new MultiCollection(DataUtilities.createType("testType","a:Integer"), List())
      multi.getSchema should beEqualTo(DataUtilities.createType("testType","a:Integer"))
    }
  }
}
