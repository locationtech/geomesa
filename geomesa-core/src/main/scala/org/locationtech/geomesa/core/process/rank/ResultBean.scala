/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

import java.util

import scala.beans.BeanProperty
import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Raw count values
 * @param route number of features that occur along the route
 * @param box number of features that occur within the surrounding square bounding box
 */
case class Counts(@BeanProperty route: Int,
                  @BeanProperty box: Int)
/**
 * CellsCovered describes how many individual cells are covered by the feature if the square bounding box is divided
 * into a grid of small cells
 * @param box the number of cells covered by the feature within the square bounding box around the route
 * @param route the number of cells covered by the feature along the route
 * @param percentageOfRouteCovered the percentage of the cells along the route that are covered by the feature
 * @param avgPerRouteCell the average number of features along the route per route cell
 */
case class CellsCovered(@BeanProperty box: Int, @BeanProperty route: Int,
                        @BeanProperty percentageOfRouteCovered: Double, @BeanProperty avgPerRouteCell: Double)

/**
 * RouteCellDeviation measures the deviation of the number of features at each location along the route. The idea is
 * that a feature that is evenly spread along the route might be more interesting than one that is located entirely at
 * one point that happens to coincide with the route
 * @param stddev standard deviation of the number of features in each cell along the route
 * @param scaledStddev the standard deviation, scaled by CellsCovered.avgPerTubeCell to normalize it
 * @param deviationScore Math.exp(-1.0 * scaledTubeCellStddev) so that low deviations have a score near 1,
 *                       and high deviations near 0
 */
case class RouteCellDeviation(@BeanProperty stddev: Double, @BeanProperty scaledStddev: Double,
                              @BeanProperty deviationScore: Double)

/**
 * Some frequency scores meant to mimic the classic tf*idf scores computed for terms and documents
 * @param idf Math.log(number of grid cells in square bounding box) / number of grid cells covered by the feature
 * @param tfIdf Counts.route * idf
 * @param scaledTfIdf scaledTfIdf = tfIdf / (the number actual tube cells), so that when comparing rankings from two
 *                    searchs a longer route is not necessarily advantageous
 */
case class TfIdf(@BeanProperty idf: Double, @BeanProperty tfIdf: Double, @BeanProperty scaledTfIdf: Double)

/**
 * Some combined scores that roll-up the other categories into a single convenient number
 * @param scoreNoMotion Geometric mean of scaledTfIdf, percentageOfTubeCellsCovered, and tubeCellDeviationScore. Is this
 *                      the best / correct combined ranking score? I think it probably just depends on what you want but
 *                      it seems reasonable.
 * @param score Geometric mean of scoreNoMotion, Math.log(motionEvidence.total + 1.0), and motionEvidence.max
 */
case class Combined(@BeanProperty scoreNoMotion: Double, @BeanProperty score: Double)

/**
 * All the values that we could think to compute to rank a group of features along a route, relative to other groups
 * @param key the key that is common among the feature group
 * @param counts
 * @param cellsCovered
 * @param routeCellDeviation
 * @param tfIdf
 * @param motionEvidence
 * @param combined
 */
case class RankingValuesBean(@BeanProperty key: String, @BeanProperty counts: Counts,
                             @BeanProperty cellsCovered: CellsCovered,
                             @BeanProperty routeCellDeviation: RouteCellDeviation, @BeanProperty tfIdf: TfIdf,
                             @BeanProperty motionEvidence: EvidenceOfMotion, @BeanProperty combined: Combined) {
  def toRankingValues(gridSize: Int, nRouteGridCells: Int): RankingValues = {
    RankingValues(counts.route, counts.box, cellsCovered.box, cellsCovered.route, routeCellDeviation.stddev,
      motionEvidence, gridSize, nRouteGridCells)
  }
}

object RankingValuesBean {
  def apply(key: String, rv: RankingValues): RankingValuesBean =
    RankingValuesBean(
      key,
      Counts(rv.tubeCount, rv.boxCount),
      CellsCovered(rv.boxCellsCovered, rv.tubeCellsCovered, rv.percentageOfTubeCellsCovered, rv.avgPerTubeCell),
      RouteCellDeviation(rv.tubeCellsStddev, rv.scaledTubeCellStddev, rv.tubeCellDeviationScore),
      TfIdf(rv.idf, rv.tfIdf, rv.scaledTfIdf),
      EvidenceOfMotion(rv.motionEvidence.total, rv.motionEvidence.max, rv.motionEvidence.stddev),
      Combined(rv.combinedScoreNoMotion, rv.combinedScore)
    )
}

/**
 * Container for the results of a search and rank process.
 * @param results a list of results sorted according to the "sortBy" field
 * @param maxScore the maximum score from the entire list of results
 * @param gridSize the grid size used to compute statistics about coverage (e.g. how many grid cells is the box divided
 *                 into along each axis, x and y?)
 * @param nRouteGridCells how many of the grid cells (total grid cells = gridSize * gridSize) are covered by the route?
 * @param sortBy which field are the results sorted by?
 */
case class ResultBean(@BeanProperty results: java.util.List[RankingValuesBean], @BeanProperty maxScore: Double,
                      @BeanProperty gridSize: Int, @BeanProperty nRouteGridCells: Int, @BeanProperty sortBy: String) {
  def keyMap = results.asScala.groupBy(_.key)

  def merge(other: ResultBean): ResultBean = {
    ResultBean.fromRankingValues(
      (results.asScala ++ other.results.asScala) // put all result together
        .groupBy(_.key)                          // now a Map[String, Buffer[RankingValuesBean]
        .mapValues(buffRVB => {                  // merge all beans in each buffer independently
          buffRVB.foldLeft(RankingValues.emptyOne(gridSize, nRouteGridCells)) { case (comb, curr) =>
            comb.merge(curr.toRankingValues(gridSize, nRouteGridCells))
          }
        }),
      sortBy
    )
  }
}

object ResultBean {
  def fromRankingValues(rankingValues: Map[String,RankingValues], sortBy: String, skip: Int = 0, max: Int = -1) = {
    if (rankingValues.isEmpty) {
      emptyResult(sortBy)
    } else {
      val firstRankingValue = rankingValues.head._2
      val (nTubeCells, gridSize) = (firstRankingValue.nTubeCells, firstRankingValue.gridDivisions)
      ResultBean(
        new util.ArrayList(
          rankingValues
            .map { case (key, rv) => RankingValuesBean(key, rv) }
            .toList
            .sortBy(_.combined.score * -1.0)
            .slice(skip, if (max > 0) skip + max else Int.MaxValue)
            .asJava
        ),
        rankingValues.maxBy(_._2.combinedScore)._2.combinedScore,
        gridSize,
        nTubeCells,
        sortBy
      )
    }
  }

  def emptyResult(sortBy: String): ResultBean =
    ResultBean(List[RankingValuesBean]().asJava, 0.0, 0, 0, sortBy)
}

