package org.locationtech.geomesa.core.process.rank

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

/**
 * Created by nhamblet.
 */
@RunWith(classOf[JUnitRunner])
class RouteAndSurroundingFeaturesTest extends Specification {

  "RankingValues" should {

    val epsilon = 0.0005
    def be_~(d: Double) = beCloseTo(d, epsilon)

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

      rv.idf must be_~(1.661) // log(100/19)
      rv.tfIdf must be_~(31.554) // 19*log(100/19)
      rv.avgPerTubeCell must be_~(1.0) // hits every cell exactly once
      rv.scaledTubeCellStddev must beCloseTo(tubeCellsStddev, epsilon) // specified stddev was 0.0
      rv.tubeCellDeviationScore must be_~(1.0) // exp(-1 * previous scaledTubeCellStddev)
      rv.scaledTfIdf must be_~(1.661) // every tube cell once, so the same as idf
      rv.percentageOfTubeCellsCovered must be_~(1.0) // hits all tube cells
      rv.combinedScoreNoMotion must be_~(1.184)
      rv.combinedScore must be_~(6.489)
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

      rv.idf must be_~(1.609) // log(100/20)
      rv.tfIdf must be_~(8.047) // 5*log(100/20)
      rv.avgPerTubeCell must be_~(0.263) // 5 / 19
      rv.scaledTubeCellStddev must be_~(2.280) // stddev / 0.263
      rv.tubeCellDeviationScore must be_~(0.102) // exp(-1 * previous scaledTubeCellStddev)
      rv.scaledTfIdf must be_~(0.424) // 5/19 * idf
      rv.percentageOfTubeCellsCovered must be_~(0.211) // 4 / 19
      rv.combinedScoreNoMotion must be_~(0.209) // less than the once-per-tubecell test, which is good
      rv.combinedScore must be_~(3.640) // again, comfortably less than the once-per-tubecell test
    }

    "merge sensibly" in {
      val rv1 = RankingValues(5, 29, 20, 4, 0.6, EvidenceOfMotion(100.0, 50.0, 1.0), 10, 19)
      val rv2 = RankingValues(3, 31, 18, 2, 0.8, EvidenceOfMotion( 50.0, 30.0, 2.0), 10, 19)
      val rvMerged = rv1.merge(rv2)

      rvMerged.tubeCount must_== 8
      rvMerged.boxCount must_== 60
      rvMerged.boxCellsCovered must_== 38
      rvMerged.tubeCellsCovered must_== 6
      rvMerged.tubeCellsStddev must be_~(1.0) // accidental pythagorean triple!
      rvMerged.motionEvidence.total must be_~(150.0)
      rvMerged.motionEvidence.max must be_~(50.0)
      rvMerged.motionEvidence.stddev must be_~(2.236)
      rvMerged.gridDivisions must_== 10
      rvMerged.nTubeCells must_== 19
    }
  }
}
