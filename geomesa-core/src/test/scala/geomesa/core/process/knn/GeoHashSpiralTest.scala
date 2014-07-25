package geomesa.core.process.knn

import geomesa.core._
import geomesa.utils.text.WKTUtils
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoHashSpiralTest extends Specification {

  def generateCvilleSF = {
    val sftName = "geomesaKNNTestQueryFeature"

    val sft = DataUtilities.createType(sftName, index.spec)

    val cvilleSF = SimpleFeatureBuilder.build(sft, List(), "equator")
    cvilleSF.setDefaultGeometry(WKTUtils.read(f"POINT(-78.4953560 38.0752150 )"))
    cvilleSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    cvilleSF
  }

  "Geomesa GeoHashSpiral PriorityQueue" should {

    "order GeoHashes correctly around Charlottesville" in {
      val cvilleSF = generateCvilleSF

      val cvillePQ = GeoHashSpiral(cvilleSF, 500.0, 5000.0)

      val cvillePQ2List = cvillePQ.toList

      val nearest9ByCalculation = cvillePQ2List.take(9).map{_.hash}

      // the below are ordered by the cartesian distances, NOT the geodetic distances
      val nearest9ByVisualInspection = List (
      "dqb0tg",
      "dqb0te",
      "dqb0tf",
      "dqb0td",
      "dqb0tu",
      "dqb0ts",
      "dqb0tc",
      "dqb0t9",
      "dqb0tv")
      nearest9ByCalculation must equalTo(nearest9ByVisualInspection)
    }


    "use the statefulDistanceFilter around Charlottesville correctly before pulling GeoHashes" in {
      val cvilleSF = generateCvilleSF

      val cvillePQ = GeoHashSpiral(cvilleSF, 500.0, 10000.0)
      cvillePQ.mutateFilterDistance(1000.0)  // units are meters

      val numHashesAfterFilter = cvillePQ.toList.length

      numHashesAfterFilter must equalTo(13)
    }


    "use the statefulDistanceFilter around Charlottesville correctly after pulling GeoHashes " in {
      val cvilleSF = generateCvilleSF

      val cvillePQ = GeoHashSpiral(cvilleSF, 500.0, 10000.0)

      // take the 20 closest GeoHashes
      val ghBeforeFilter = cvillePQ.take(20)

      ghBeforeFilter.length must equalTo(20)

      // now mutate the filter -- this is restrictive enough that no further GeoHashes should pass
      cvillePQ.mutateFilterDistance(1000.0)  // units are meters

      // attempt to take five more
      val ghAfterFilter =  cvillePQ.take(5)

      ghAfterFilter.length must equalTo(0)
    }
  }
}
