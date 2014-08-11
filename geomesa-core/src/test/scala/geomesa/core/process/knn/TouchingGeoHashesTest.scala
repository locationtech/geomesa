package geomesa.core.process.knn

import geomesa.utils.geohash.GeoHash
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TouchingGeoHashesTest extends Specification {

  def generateCvilleGH = {
    val precision = 30
    val lat = 38.0752150
    val lon = -78.4953560
    GeoHash(lon, lat, precision)
  }

  def generateSuvaGH = {
    val precision = 10
    val lat = -18.140
    val lon = 178.440
    GeoHash(lon, lat, precision)
  }

  def generateMcMurdoGH = {
    val precision = 5
    val lat = -77.842
    val lon = 166.68360
    GeoHash(lon, lat, precision)
  }

  "Geomesa TouchingGeoHashes" should {

    "find GeoHashes  around Charlottesville, Virginia" in {
      val touchingByCalculation = TouchingGeoHashes.touching(generateCvilleGH).map ( _.hash )
      val touchingByVisualInspection = List(
        "dqb0te",
        "dqb0tf",
        "dqb0td",
        "dqb0tu",
        "dqb0ts",
        "dqb0w5",
        "dqb0w4",
        "dqb0wh")
      touchingByCalculation.forall ( touchingByVisualInspection.contains ) must beTrue
    }

    "Correctly treat the antimeridian and find GeoHashes around Suva, Fiji" in {
      val touchingByCalculation = TouchingGeoHashes.touching(generateSuvaGH).map ( _.hash )
      val touchingByVisualInspection = List(
        "rv",
        "rg",
        "re",
        "rs",
        "rt",
        "2j",
        "2h",
        "25")
      touchingByCalculation.forall ( touchingByVisualInspection.contains ) must beTrue
    }

    "Correctly treat the polar region and the antimeridian and find GeoHashes around McMurdo Station" in {
      val touchingByCalculation = TouchingGeoHashes.touching(generateMcMurdoGH).map ( _.hash )
      val touchingByVisualInspection = List(
        "h",
        "j",
        "n",
        "0",
        "1",
        "4",
        "5",
        "2",
        "r",
        "q")
      touchingByCalculation.forall ( touchingByVisualInspection.contains ) must beTrue
    }
  }
}
