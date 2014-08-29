/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.process.rank

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.geotools.geometry.jts.GeometryBuilder

import scala.beans.BeanProperty

/**
 * This is an aggregate of the motion scores from a series of tracklets, for a single ID / entity. Basically we just
 * calculate the aggregate motion score for many tracklets and them compute the total, maximum, and standard deviation.
 * @param total
 * @param max
 * @param stddev
 */
case class EvidenceOfMotion(@BeanProperty total: Double, @BeanProperty max: Double, @BeanProperty stddev: Double)

/**
 * This object contains values used to rank an entity along a route or track
 * @param tubeCount the number of occurrences of the entity along the route
 * @param boxCount the number of occurrences of the entity within a square box that bounds the route (for context)
 * @param boxCellsCovered once the square bounding box of the route is gridded to discrete cells, how many cells are
 *                        covered by the entity?
 * @param tubeCellsCovered how many of the cells both intersect the route and are covered by the entity?
 * @param tubeCellsStddev what is the standard deviation of the count of the entity in each cell along the route? This
 *                        is intended to measure whether the entity is concentrated in a few locations or spread along
 *                        the route
 * @param motionEvidence See above. An aggregate score about whether potential tracklets extracted from the entity's
 *                       locations show evidence of motion along the route.
 * @param gridDivisions the number of grid cells along one dimension in the square bounding box that surrounds the route
 * @param nTubeCells the number of grid cells that are intersected by the route
 */
case class RankingValues(tubeCount: Int, boxCount: Int, boxCellsCovered: Int, tubeCellsCovered: Int,
                         tubeCellsStddev: Double, motionEvidence: EvidenceOfMotion, gridDivisions: Int,
                         nTubeCells: Int) {
  /**
   * This is idf = Inverse document frequency, like is used for ranking documents from keywords. In this case, documents
   * = grid cells and terms = the number of grid cells covered by the entity
   *@return log(total number of grid cells / (# of grid cells covered))
   */
  def idf =
    if (boxCellsCovered == 0) Double.MaxValue
    else Math.log((gridDivisions * gridDivisions).toDouble / boxCellsCovered.toDouble)

  /**
   * This is the idf * the number of times that the entity occurs along the query route. Intended to be analogous to
   * tfIdf scores used to rank documents from keywords
   * @return
   */
  def tfIdf = idf * tubeCount

  /**
   * The standard deviation of the count of the entity in each grid cell along the route, normalized by dividing it by
   * the average number of occurrences in each grid cell
   * @return
   */
  def scaledTubeCellStddev = if (avgPerTubeCell > 0.0) tubeCellsStddev / avgPerTubeCell else 0.0

  /**
   * A score in the range [0.0, 1.0] calculated from the deviation of the entity along the route. A score of 1 means the
   * entity is well spread along the route and a score of 0 means it is concentrated in one location.
   * @return score between 0 and 1
   */
  def tubeCellDeviationScore =  Math.exp(-1.0 * scaledTubeCellStddev)

  /**
   * The average number of entity instances per cell, only considering observations and cells that intersect with the
   * route
   * @return
   */
  def avgPerTubeCell = tubeCount.toDouble / nTubeCells.toDouble

  /**
   * The tfIdf score scaled by the average number of observations per cell.
   * @return
   */
  def scaledTfIdf = idf * avgPerTubeCell

  /**
   * The percentage of cells along the route that are covered by an entity observation
   * @return
   */
  def percentageOfTubeCellsCovered = tubeCellsCovered.toDouble / nTubeCells.toDouble

  /**
   * The combined score, not including the motion score, which is the geometric mean of the normalized tfIdf, the
   * deviation score, and the percentage of tube cells covered.
   * @return
   */
  def combinedScoreNoMotion = Math.pow(scaledTfIdf * percentageOfTubeCellsCovered * tubeCellDeviationScore, 1.0 / 3.0)

  /**
   * Aggregate the combined score without motion with the motion evidence score.
   * @return (combinedScoreNoMotion * log(total motion evidence + 1.0) * (maximum motion evidence))^^(1/3)
   */
  def combinedScore =
    if (motionEvidence.total > 0.0)
      Math.pow(combinedScoreNoMotion * Math.log(motionEvidence.total + 1.0) * motionEvidence.max, 1.0 / 3.0)
    else 0.0

  /**
   * Merge one RankingValues object with another
   * @param other
   * @return
   */
  def merge(other: RankingValues) =
    RankingValues(
      tubeCount + other.tubeCount,
      boxCount + other.boxCount,
      boxCellsCovered + other.boxCellsCovered,
      tubeCellsCovered + other.tubeCellsCovered,
      MathUtil.combineStddev(tubeCellsStddev, other.tubeCellsStddev), // assumes independence, which might be questionable
      EvidenceOfMotion(
        motionEvidence.total + other.motionEvidence.total,
        Math.max(motionEvidence.max, other.motionEvidence.max),
        MathUtil.combineStddev(motionEvidence.stddev, other.motionEvidence.stddev)
      ),
      gridDivisions,
      nTubeCells
    )
}

object RankingValues {
  def emptyOne(gridDivisions: Int, nTubeCells: Int) = RankingValues(0, 0, 0, 0, 0,
    EvidenceOfMotion(0, 0, 0), gridDivisions, nTubeCells)
}

object RankingDefaults {
  final val defaultEvidenceOfMotion = EvidenceOfMotion(0.0, 0.0, 0.0)
  final val defaultRouteDivisions = 100.0
  final val defaultGridDivisions = 100
  final val maxTimeBetweenPings = 60 * 60 // one hour in seconds
  final val defaultSkipResultsStr = "0"
  final val defaultMaxResultsStr = "1000"
  final val defaultResultsSortField = RankingValuesBean.NamedScore.DEFAULT_SCORE
}

/**
 * Pairs a route with all the simple features along it, and surrounding it
 * @param route
 * @param boxFeatures surrounding features
 * @param tubeFeatures features along route
 */
class RouteAndSurroundingFeatures(val route: Route,
                                  val boxFeatures: SimpleFeatureWithDateTimeAndKeyCollection,
                                  val tubeFeatures: SimpleFeatureWithDateTimeAndKeyCollection) {

  /**
   * Computes evidence of motion for all IDs / entities in data set
   * @return Map where key = ID and value = evidence of motion for that key
   */
  def evidenceOfMotion: Map[String,EvidenceOfMotion] = {
    val tubeFeatureMap = tubeFeatures.groupByKey
    val boxFeatureMap = boxFeatures.groupByKey
    boxFeatureMap.keys.map(key => key -> (tubeFeatureMap.get(key) match {
      case None => RankingDefaults.defaultEvidenceOfMotion
      case Some(sfs) => evidenceOfMotion(sfs, boxFeatureMap(key))
    })).toMap
  }

  def hasDateTimeInSpec(sf: SimpleFeatureWithDateTimeAndKey, collection: SimpleFeatureWithDateTimeAndKeyCollection) =
    sf.dateTime(collection.spec).isDefined

  def hasDateTimeInBoxSpec(sf: SimpleFeatureWithDateTimeAndKey) =
    hasDateTimeInSpec(sf, boxFeatures)

  // unchecked get, use with caution
  def dateTimeInSpec(sf: SimpleFeatureWithDateTimeAndKey, collection: SimpleFeatureWithDateTimeAndKeyCollection) =
    sf.dateTime(collection.spec).get

  def dateTimeInBoxSpec(sf: SimpleFeatureWithDateTimeAndKey) =
    dateTimeInSpec(sf, boxFeatures)

  /**
   * This computes evidence of motion for a single ID / entity. First, it groups all observations into potential
   * tracklets based on time and location. Then it calculates motion scores for each tracklet. Then it aggregates all
   * the motion scores into statistics including the total, max, and standard deviation
   * @param tubeFeatures
   * @param boxFeatures
   * @param routeDivisions
   * @return
   */
  def evidenceOfMotion(tubeFeatures: Iterable[SimpleFeatureWithDateTimeAndKey],
                       boxFeatures: Iterable[SimpleFeatureWithDateTimeAndKey],
                       routeDivisions: Double = RankingDefaults.defaultRouteDivisions): EvidenceOfMotion = {
    val placeTimes =
      boxFeatures
        .collect {
          case sf if hasDateTimeInBoxSpec(sf) => new CoordWithDateTime(sf.centroidCoordinate, dateTimeInBoxSpec(sf))
        }
        .toList
        .sortBy(_.dt.getMillis)
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

  /**
   * Input is an iterable over a list of cells. Each item in the sequence is a map from ID / entity to the count of the
   * ID in that cell. This computes the total for each ID / entity across the iterable.
   * @param m
   * @return Map[String,Int] where String is the ID and Int is the total for the ID across the Iterable
   */
  private def aggregateCellCounts(m: Iterable[Map[String,Int]]) = m.foldLeft(Map[String,Int]())((all, one) => all ++
    one.map { case(k, v) => k -> (v + all.getOrElse(k, 0))})

  /**
   * Ranks all the entities, grouped by identifier, found in the feature set
   * @param boxEnvelope the envelope that defines the square box around the route, which provides ranking context
   * @param routeBufferShapes the shapes that define the buffered route
   * @param gridDivisions the number of divisions to break the route into, for coverage and motion calculations
   * @return Map[String,RankingValues] where the key is the ID of the entities
   */
  def rank(boxEnvelope: Envelope, routeBufferShapes: List[Geometry],
           gridDivisions: Int = RankingDefaults.defaultGridDivisions): Map[String,RankingValues] = {
    val geomFactory = new GeometryBuilder()

    // Find the list of IDs in the data
    val tubeHexIds = tubeFeatures.countKeys
    val boxHexIds = boxFeatures.countKeys
    val combined = (tubeHexIds.keySet ++ boxHexIds.keySet).map(k =>
      (k, (tubeHexIds.getOrElse(k, 0), boxHexIds.getOrElse(k, 0)))).toMap

    // Break the bounding box into a grid
    val grid = new Grid(boxEnvelope, 100)
    val points = grid.getIndexPairsWithLatLons

    // Figure out which grid points from the bounding box intersect the route
    val tubePoints = points.filter { pnt =>
      val point = geomFactory.point(pnt._2._1, pnt._2._2)
      routeBufferShapes.exists(polyg => polyg.intersects(point))
    }.map(_._1).toSet

    // Find out how many of each entity occur in each grid cell
    val gridCounts = boxFeatures.gridCounts(grid)
    val tubeCounts = gridCounts.filterKeys(tubePoints.contains)

    // The "binary map" is simply used to compute how many grid cells are covered rather than how many observations
    // occur in each grid cell
    def toBinaryMap(maps: Iterable[Map[String,Int]]) = maps.map(m => m.mapValues(v => if (v > 1) 1 else v))
    val allMaps = gridCounts.values
    val binaryMaps = toBinaryMap(allMaps)
    val tubeMaps = tubeCounts.values
    val tubeBinaryMaps = toBinaryMap(tubeMaps)
    val cellsCovered = aggregateCellCounts(binaryMaps)
    val tubeCellsCovered = aggregateCellCounts(tubeBinaryMaps)
    // What is the standard deviation of the number of observations across the grid cells along the route?
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


