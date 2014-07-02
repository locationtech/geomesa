package geomesa.core.process.knn

import collection.JavaConversions._
import geomesa.core._
import geomesa.utils.text.WKTUtils
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SomeGeoHashesTest extends Specification {

  sequential

  def generateCCRiSF = {
    val sftName = "geomesaKNNTestQueryFeature"
    val sft = DataUtilities.createType(sftName, index.spec)

    val ccriSF = SimpleFeatureBuilder.build(sft, List(), "equator")
    ccriSF.setDefaultGeometry(WKTUtils.read(f"POINT(-78.4953560 38.0752150 )"))
    ccriSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    ccriSF
  }

  "Geomesa SomeGeoHashes PriorityQueue" should {

    "order GeoHashes correctly around CCRi" in {
      val ccriSF = generateCCRiSF
      val ccriPQ = SomeGeoHashes(ccriSF, 1000.0, 1000.0)
      ccriPQ.exhaustIterator() // call this so that the PriorityQueue can order ALL geohashes
      val ccriPQ2List = ccriPQ.toList()
      val nearest9ByCalculation = ccriPQ2List.take(9).map{_.hash}

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


    "use the statefulDistanceFilter around CCRi correctly" in {
      val ccriSF = generateCCRiSF
      val ccriPQ = SomeGeoHashes(ccriSF, 1000.0, 1000.0)
      ccriPQ.updateDistance(0.004)  // units are degrees, so distance is cartesian
      ccriPQ.exhaustIterator() // call this so that the PriorityQueue can order ALL geohashes
      val numHashesAfterFilter = ccriPQ.toList().length
      numHashesAfterFilter must equalTo(6)
    }


    "use the statefulDistanceFilter around CCRi correctly when the PriorityQueue is fully loaded" in {
      val ccriSF = generateCCRiSF
      val ccriPQ = SomeGeoHashes(ccriSF, 1000.0, 1000.0)
      ccriPQ.exhaustIterator() // call this so that the PriorityQueue can order ALL geohashes
      ccriPQ.updateDistance(0.004)  // units are degrees, so distance is cartesian
      val numHashesAfterFilter = ccriPQ.toList().length
      numHashesAfterFilter must equalTo(6)

    }

  }
}
