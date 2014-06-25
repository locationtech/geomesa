package geomesa.core.process.rank

import java.util

import scala.beans.BeanProperty
import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/23/14
 * Time: 3:23 PM
 */
case class Counts(@BeanProperty route: Int, @BeanProperty box: Int)
case class CellsCovered(@BeanProperty box: Int, @BeanProperty route: Int,
                        @BeanProperty percentageOfRouteCovered: Double, @BeanProperty avgPerRouteCell: Double)
case class RouteCellDeviation(@BeanProperty stddev: Double, @BeanProperty scaledStddev: Double,
                              @BeanProperty deviationScore: Double)
case class TfIdf(@BeanProperty idf: Double, @BeanProperty tfIdf: Double, @BeanProperty scaledTfIdf: Double)
case class Combined(@BeanProperty scoreNoMotion: Double, @BeanProperty score: Double)
case class RankingValuesBean(@BeanProperty key: String, @BeanProperty counts: Counts,
                             @BeanProperty cellsCovered: CellsCovered,
                             @BeanProperty routeCellDeviation: RouteCellDeviation, @BeanProperty tfIdf: TfIdf,
                             @BeanProperty motionEvidence: EvidenceOfMotion, @BeanProperty combined: Combined) {
  def toRankingValues(gridSize: Int, nRouteGridCells: Int): RankingValues = {
    RankingValues(counts.route, counts.box, cellsCovered.box, cellsCovered.route, routeCellDeviation.stddev,
      motionEvidence, gridSize, nRouteGridCells)
  }
}

case class ResultBean(@BeanProperty results: java.util.List[RankingValuesBean], @BeanProperty maxScore: Double,
                      @BeanProperty gridSize: Int, @BeanProperty nRouteGridCells: Int, @BeanProperty sortBy: String) {
  def keyMap = results.asScala.groupBy(_.key)

  def merge(other: ResultBean): ResultBean = {
    val km1 = keyMap
    val km2 = other.keyMap
    val allKeys = km1.keySet ++ km2.keySet
    def emptyBuffer = mutable.Buffer[RankingValuesBean]()
    val valueLists = allKeys.map(k => (k, km1.getOrElse(k, emptyBuffer) ++ km2.getOrElse(k, emptyBuffer)))
    ResultBean.fromRankingValues(valueLists.map {
      case (key, resultList) =>
        (key, resultList.foldLeft(RankingValues.emptyOne(gridSize, nRouteGridCells)) {
          case (combined, current) => combined.merge(current.toRankingValues(gridSize, nRouteGridCells))
        })
    }.toMap, sortBy)
  }
}

object ResultBean {
  def fromRankingValues(rankingValues: Map[String,RankingValues], sortBy: String, skip: Int = 0, max: Int = -1) = {
    val (nTubeCells, gridSize) = rankingValues.headOption.
      flatMap(h => Some((h._2.nTubeCells, h._2.gridDivisions))).getOrElse((0,0))
    ResultBean(new util.ArrayList(rankingValues.map {
      case (key, rv) => RankingValuesBean(key, Counts(rv.tubeCount, rv.boxCount),
        CellsCovered(rv.boxCellsCovered, rv.tubeCellsCovered, rv.percentageOfTubeCellsCovered, rv.avgPerTubeCell),
        RouteCellDeviation(rv.tubeCellsStddev, rv.scaledTubeCellStddev, rv.tubeCellDeviationScore),
        TfIdf(rv.idf, rv.tfIdf, rv.scaledTfIdf),
        EvidenceOfMotion(rv.motionEvidence.total, rv.motionEvidence.max, rv.motionEvidence.stddev),
        Combined(rv.combinedScoreNoMotion, rv.combinedScore))
    }.toList.sortBy(_.combined.score * -1.0).
      slice(skip, if (max > 0) skip + max else Int.MaxValue).asJava),
      rankingValues.maxBy(_._2.combinedScore)._2.combinedScore, gridSize, nTubeCells, sortBy)
  }
}

