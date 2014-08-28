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

import com.vividsolutions.jts.geom.LineString
import org.locationtech.geomesa.core.process.query.QueryProcess
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.apache.log4j.Logger
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.JTS
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS

import scala.util.Try

/**
 * This delegates the proximity search for a route to the QueryProcess class, then applies the ranking algorithm .
 */
@DescribeProcess(
title = "Rank Features along Route", // "Geomesa-enabled Ranking of Feature Groups in Proximity to Route",
description = "Performs a proximity search on a Geomesa feature collection using another feature collection as input." +
  " Then groups the features according to a key and computes ranking metrics that measure the prominence of " +
  "each key within the search region. The computed metrics measure the frequency of each feature group within the " +
  "search region, relative frequency in the surrounding area, the spatial diversity of the feature within the " +
  "region, and evidence of motion through the search region."
)
class RouteRankProcess {

  private val log = Logger.getLogger(classOf[RouteRankProcess])

  /**
   *
   * @param inputFeatures The features that define the query route.
   * @param dataFeatures The feature layer to query along the route/
   * @param bufferDistance The distance in meters to buffer the route for the query
   * @param keyField The key field to group the matching observations by
   * @param skip The number of results to skip (for paging of results)
   * @param max The maximum number of results to return
   * @param sortBy The field to sort the results by (combined.score by default)
   * @return A list of entity IDs and ranking scores computed for each one
   */
  @DescribeResult(description = "Output ranking scores")
  def execute(
               @DescribeParameter(
                 name = "inputFeatures",
                 description = "This must be a single line string for now")
               inputFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "dataFeatures",
                 description = "The data set to query for matching features")
               dataFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "bufferDistance",
                 description = "Buffer size in meters")
               bufferDistance: java.lang.Double,

               @DescribeParameter(
                 name = FeatureGroupRanker.KEY_FIELD_NAME,
                 description = FeatureGroupRanker.KEY_FIELD_DESCRIPTION)
               keyField: String,

               @DescribeParameter(
                 name = FeatureGroupRanker.SKIP_FIELD_NAME,
                 min = 0,
                 defaultValue = RankingDefaults.defaultSkipResultsStr,
                 description = FeatureGroupRanker.SKIP_FIELD_DESCRIPTION)
               skip: Int,

               @DescribeParameter(
                 name = FeatureGroupRanker.MAX_FIELD_NAME,
                 min = 0,
                 defaultValue = RankingDefaults.defaultMaxResultsStr,
                 description = FeatureGroupRanker.MAX_FIELD_DESCRIPTION)
               max: Int,

               @DescribeParameter(
                 name = FeatureGroupRanker.SORT_BY_FIELD_NAME,
                 min = 0,
                 defaultValue = RankingDefaults.defaultResultsSortField,
                 description = FeatureGroupRanker.SORT_BY_FIELD_DESCRIPTION)
               sortBy: String

               ): ResultBean = {
    new RouteFeatureGroupRanker(inputFeatures, dataFeatures, bufferDistance, keyField, skip, max, sortBy).groupAndRank
  }
}

class RouteFeatureGroupRanker(inputFeatures: SimpleFeatureCollection,
                              override val dataFeatures: SimpleFeatureCollection,
                              override val bufferMeters: Double,
                              override val keyField: String,
                              override val skip: Int,
                              override val max: Int,
                              override val sortBy: String) extends FeatureGroupRanker {
  override def queryRoute(route: Route) = {
    val routeShape = route.route.bufferMeters(bufferMeters)
    val ff = CommonFactoryFinder.getFilterFactory2
    val routeFilter = ff.intersects(ff.property(dataFeatures.getSchema.getGeometryDescriptor.getLocalName),
      ff.literal(routeShape))
    val qp = new QueryProcess
    qp.execute(dataFeatures, routeFilter)
  }

  override def extractRoute = {
    // Currently only works for a single line string
    if (inputFeatures.size() == 1) {
      val routeTry = for {
        ls <- Try(inputFeatures.features().take(1).next().getDefaultGeometry.asInstanceOf[LineString])
        ls4326 <- wgs84LineString(ls)
        route = new Route(ls4326)
      } yield route
      routeTry.toOption
    }
    else None
  }

  def wgs84LineString(ls: LineString) = {
    Try(if (ls.getSRID == 4326) ls else {
      val sourceCRS = inputFeatures.getSchema.getCoordinateReferenceSystem
      val transform = CRS.findMathTransform(sourceCRS, DefaultGeographicCRS.WGS84, true)
      JTS.transform(ls, transform).asInstanceOf[LineString]
    })
  }
}
