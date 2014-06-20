package geomesa.core.process.rank

import com.vividsolutions.jts.geom.{Geometry, Envelope, Polygon}
import org.geotools.geometry.jts.GeometryBuilder

import scala.beans.BeanProperty

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/18/14
 * Time: 10:44 AM
 */

case class EvidenceOfMotion(@BeanProperty total: Double, @BeanProperty max: Double, @BeanProperty stddev: Double)

case class RankingValues(tubeCount: Int, boxCount: Int, boxCellsCovered: Int, tubeCellsCovered: Int,
                         tubeCellsStddev: Double, motionEvidence: EvidenceOfMotion, gridDivisions: Int,
                         nTubeCells: Int) {
  def idf = if (boxCellsCovered == 0) Double.MaxValue else Math.log((gridDivisions * gridDivisions).toDouble /
    boxCellsCovered.toDouble)
  def tfIdf = idf * tubeCount
  def scaledTubeCellStddev = if (avgPerTubeCell > 0.0) tubeCellsStddev / avgPerTubeCell else 0.0
  def tubeCellDeviationScore =  Math.exp(-1.0 * scaledTubeCellStddev)
  def avgPerTubeCell = tubeCount.toDouble / nTubeCells.toDouble
  def scaledTfIdf = idf * avgPerTubeCell
  def percentageOfTubeCellsCovered = tubeCellsCovered.toDouble / nTubeCells.toDouble
  def combinedScoreNoMotion = Math.pow(scaledTfIdf * percentageOfTubeCellsCovered * tubeCellDeviationScore, 1.0 / 3.0)
  def combinedScore = if (motionEvidence.total > 0.0)
    Math.pow(combinedScoreNoMotion * Math.log(motionEvidence.total + 1.0) * motionEvidence.max, 1.0 / 3.0) else 0.0
}

object RankingDefaults {
  final val defaultEvidenceOfMotion = EvidenceOfMotion(0.0, 0.0, 0.0)
  final val defaultRouteDivisions = 100.0
  final val defaultGridDivisions = 100
  final val maxTimeBetweenPings = 60 * 60 // one hour in seconds
  final val defaultMaxResultsStr = "1000"
  final val defaultResultsSortField = "combined.score"
}

class RouteAndSurroundingFeatures(val route: Route,
                                  val boxFeatures: SimpleFeatureWithDateTimeAndKeyCollection,
                                  val tubeFeatures: SimpleFeatureWithDateTimeAndKeyCollection) {

  def evidenceOfMotion: Map[String,EvidenceOfMotion] = {
    val tubeFeatureMap = tubeFeatures.groupByKey
    val boxFeatureMap = boxFeatures.groupByKey
    boxFeatureMap.keys.map(key => key -> (tubeFeatureMap.get(key) match {
      case None => RankingDefaults.defaultEvidenceOfMotion
      case Some(sfs) => evidenceOfMotion(sfs, boxFeatureMap(key))
    })).toMap
  }

  def evidenceOfMotion(tubeFeatures: Iterable[SimpleFeatureWithDateTimeAndKey],
                       boxFeatures: Iterable[SimpleFeatureWithDateTimeAndKey],
                       routeDivisions: Double = RankingDefaults.defaultRouteDivisions): EvidenceOfMotion = {
    val placeTimes = boxFeatures.map(sf => (sf.centroidCoordinate, sf.dateTime)).filter(_._2.isDefined).
      map(p => new CoordWithDateTime(p._1, p._2.get)).toList.sortBy(_.dt.getMillis)
    val motionSets = placeTimes.foldLeft(List[List[CoordWithDateTime]](List[CoordWithDateTime]())) {
      case(bigList, currentPoint) =>
        val first = bigList.head
        val rest = bigList.tail
        if (currentPoint.consistentWithMotion(first)) {
          List(currentPoint :: first) ++ rest
        }
        else List(List(currentPoint)) ++ bigList
    }.map(ms => CoordSequence.fromCoordWithDateTimeList(ms))
    val motionScores = motionSets.filter(ms => ms.coords.size > 0).
      map(ms => route.motionScores(ms, routeDivisions).combined)
    if (motionScores.size > 0) EvidenceOfMotion(motionScores.sum, motionScores.max,
      MathUtil.stdDev(motionScores, motionScores.sum / motionScores.size)) else RankingDefaults.defaultEvidenceOfMotion
  }

  private def aggregateCellCounts(m: Iterable[Map[String,Int]]) = m.foldLeft(Map[String,Int]())((all, one) => all ++
    one.map { case(k, v) => k -> (v + all.getOrElse(k, 0))})

  def rank(boxEnvelope: Envelope, routeBufferShapes: List[Geometry],
           gridDivisions: Int = RankingDefaults.defaultGridDivisions): Map[String,RankingValues] = {
    val geomFactory = new GeometryBuilder()
    val tubeHexIds = tubeFeatures.countKeys
    val boxHexIds = boxFeatures.countKeys
    val combined = (tubeHexIds.keySet ++ boxHexIds.keySet).map(k =>
      (k, (tubeHexIds.getOrElse(k, 0), boxHexIds.getOrElse(k, 0)))).toMap

    val grid = new Grid(boxEnvelope, 100)
    val points = grid.getIndexPairsWithLatLons

    val tubePoints = points.filter { pnt =>
      val point = geomFactory.point(pnt._2._1, pnt._2._2)
      routeBufferShapes.exists(polyg => polyg.intersects(point))
    }.map(_._1).toSet

    val gridCounts = boxFeatures.gridCounts(grid)
    val tubeCounts = gridCounts.filterKeys(tubePoints.contains)

    def toBinaryMap(maps: Iterable[Map[String,Int]]) = maps.map(m => m.mapValues(v => if (v > 1) 1 else v))
    val allMaps = gridCounts.values
    val binaryMaps = toBinaryMap(allMaps)
    val tubeMaps = tubeCounts.values
    val tubeBinaryMaps = toBinaryMap(tubeMaps)
    val cellsCovered = aggregateCellCounts(binaryMaps)
    val tubeCellsCovered = aggregateCellCounts(tubeBinaryMaps)
    val tubeCellStddev = tubeMaps.flatMap(_.toList).groupBy(_._1).map { case(k,lp) =>
      val l1 = lp.map(_._2.toDouble).toList
      val l = l1 ++ (if (l1.size < tubePoints.size)
        Array.fill[Double](tubePoints.size - l1.size)(0.0).toList else List())
      k -> MathUtil.stdDev(l, MathUtil.avg(l))
    }

    val evidenceOfMotionVals = evidenceOfMotion
    combined.keys.toList.sortBy(combined(_)._1).map { hexid =>
      val (c1, c2) = combined(hexid)
      val cellsCovered1 = cellsCovered(hexid)
      val tubeCellsCovered1 = tubeCellsCovered.getOrElse(hexid, 0)
      val tubeCellStddev1 = tubeCellStddev.getOrElse(hexid, 0.0)
      val evidenceOfMotion1 = evidenceOfMotionVals.getOrElse(hexid, RankingDefaults.defaultEvidenceOfMotion)
      (hexid, RankingValues(c1, c2, cellsCovered1, tubeCellsCovered1, tubeCellStddev1, evidenceOfMotion1,
        gridDivisions, tubePoints.size))
    }.toMap
  }
}


