package geomesa.core.util

import org.geotools.data.DataUtilities
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MultiCollectionTest  extends Specification {

  "MultiCollection" should {
    "return return the proper schema" in {
      val multi = new MultiCollection(DataUtilities.createType("testType","a:Integer"), List())
      multi.getSchema should beEqualTo(DataUtilities.createType("testType","a:Integer"))
    }
  }
}
