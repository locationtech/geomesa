package org.locationtech.geomesa.core.process.rank

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.collection.JavaConversions._

/**
 * Created by nhamblet.
 */
@RunWith(classOf[JUnitRunner])
class ResultBeanTest extends Specification {

  "ResultBean" should {

    val gridDivisions = 20 // 20 x 20 grid
    val tubeCells = 120 // quarter of the grid is in the tube

    def rv(tc: Int, bc: Int, bcc: Int, tcc: Int, tcD: Double, evTot: Double, evMax: Double, evStd: Double) =
      RankingValues(tc, bc, bcc, tcc, tcD, EvidenceOfMotion(evTot, evMax, evStd), gridDivisions, tubeCells)

    val rv1 = rv(120, 300, 250, 178,   0.1, 1000.0, 900.0, 0.01) // clear winner
    val rv2 = rv( 51,   1,   1,   0, 100.0,    0.1,   0.1,  0.0) // clear loser (except by counts.route)
    val rv3 = rv( 50, 300, 120,  30,   1.0,   10.0,   5.0,  2.0) // mediocre

    "handle empty results gracefully" in {
      val empty = ResultBean.fromRankingValues(Map[String, RankingValues](), "sortField")
      empty.results.isEmpty must beTrue
      empty.maxScore must beEqualTo(Double.MinValue)
    }

    "sort results descending by combined.score" in {
      val rvMap = Map[String, RankingValues](
        "winner"  -> rv1,
        "loser"   -> rv2,
        "midling" -> rv3
        )
      val rb = ResultBean.fromRankingValues(rvMap, "combined.score")
      val rbKeys = rb.results.map(_.key).toArray
      rbKeys(0) must be("winner")
      rbKeys(1) must be("midling")
      rbKeys(2) must be("loser")
      rb.results
        .map(_.combined.score)
        .sliding(2, 1)
        .forall(x => x(0) >= x(1)) must beTrue
    }

    "sort results descending by counts.route" in {
      val rvMap = Map[String, RankingValues](
        "winner"  -> rv1,
        "loser"   -> rv2,
        "midling" -> rv3
      )
      val rb = ResultBean.fromRankingValues(rvMap, "counts.route")
      val rbKeys = rb.results.map(_.key).toArray
      rbKeys(0) must be("winner")
      rbKeys(1) must be("loser")
      rbKeys(2) must be("midling")
      rb.results
        .map(_.counts.route)
        .sliding(2, 1)
        .forall(x => x(0) >= x(1)) must beTrue
    }

    "respect skip and max parameters" in {
      val rvMap = Map[String, RankingValues](
        "winner"  -> rv1,
        "loser"   -> rv2,
        "midling" -> rv3
      )
      val rb = ResultBean.fromRankingValues(rvMap, "combined.score", 1, 1)
      rb.results.size must beEqualTo(1)
      rb.results.head.key must beEqualTo("midling")
    }
  }

  "RankingValues" should {

    val epsilon = 0.0005

    // set up a 10x10 grid, with a tube hitting the main diagonal and first super-diagonal
    // there are 10+9=19 cells along the route
    val gridDivisions = 10
    val nTubeCells = gridDivisions + (gridDivisions - 1)

    "calculate derived scores in once-per-tubecell case correctly" in {
      val tubeCellsStddev = 0.0 // hits every tube cell exactly once
      val rv = RankingValues(
        nTubeCells, nTubeCells, nTubeCells, nTubeCells, tubeCellsStddev,
        EvidenceOfMotion(100.0, 50.0, 1.0),
        gridDivisions, nTubeCells)

      rv.idf must beCloseTo(1.661, epsilon) // log(100/19)
      rv.tfIdf must beCloseTo(31.554, epsilon) // 19*log(100/19)
      rv.avgPerTubeCell must beCloseTo(1.0, epsilon) // hits every cell exactly once
      rv.scaledTubeCellStddev must beCloseTo(tubeCellsStddev, epsilon) // specified stddev was 0.0
      rv.tubeCellDeviationScore must beCloseTo(1.0, epsilon) // exp(-1 * previous scaledTubeCellStddev)
      rv.scaledTfIdf must beCloseTo(1.661, epsilon) // every tube cell once, so the same as idf
      rv.percentageOfTubeCellsCovered must beCloseTo(1.0, epsilon) // hits all tube cells
      rv.combinedScoreNoMotion must beCloseTo(1.184, epsilon)
      rv.combinedScore must beCloseTo(6.489, epsilon)
    }

    "calculate derived scores for area-but-not-tube-specifically case correctly" in {
      // suppose they show up in 1/5 of the places
      //   so 20 total, 4 on tube
      //   let's say, on the tube, they hit one of the cells twice (so, 5 occurrences in cell tubes)
      //   and, outside the tube, they hit 10 of the cells once, 4 twice, and 2 three times (10+4(2)+2(3)=24)
      val tubeCount = 5
      val boxCount = 29 // 5 on tube, 24 off tube
      val boxCellsCovered = 20 // 4 tube cells and 16 non-tube cells
      val tubeCellsCovered = 4
      val tubeCellsStddev = 0.6 // actually 0.5619..., but whatever
      val rv = RankingValues(
          tubeCount, boxCount, boxCellsCovered, tubeCellsCovered, tubeCellsStddev,
          EvidenceOfMotion(100.0, 50.0, 1.0),
          gridDivisions, nTubeCells)

      rv.idf must beCloseTo(1.609, epsilon) // log(100/20)
      rv.tfIdf must beCloseTo(8.047, epsilon) // 5*log(100/20)
      rv.avgPerTubeCell must beCloseTo(0.263, epsilon) // 5 / 19
      rv.scaledTubeCellStddev must beCloseTo(2.280, epsilon) // stddev / 0.263
      rv.tubeCellDeviationScore must beCloseTo(0.102, epsilon) // exp(-1 * previous scaledTubeCellStddev)
      rv.scaledTfIdf must beCloseTo(0.424, epsilon) // 5/19 * idf
      rv.percentageOfTubeCellsCovered must beCloseTo(0.211, epsilon) // 4 / 19
      rv.combinedScoreNoMotion must beCloseTo(0.209, epsilon) // less than the once-per-tubecell test, which is good
      rv.combinedScore must beCloseTo(3.640, epsilon) // again, comfortably less than the once-per-tubecell test
    }
  }
}
