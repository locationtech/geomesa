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
}
