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

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.core.data.AccumuloFeatureCollection
import org.locationtech.geomesa.core.index
import org.locationtech.geomesa.core.process.query.QueryProcess
import org.locationtech.geomesa.utils.geotools.Conversions._
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS

/**
 * Provides the basic functionality to rank data used by both WPS processes: RouteRank and TrackRank
 */
trait FeatureGroupRanker extends Logging {

  def dataFeatures: SimpleFeatureCollection // features to rank
  def extractRoute: Option[Route] // A method that defines the route to rank along
  def keyField: String // the field from dataFeatures to group data by
  def bufferMeters: Double // how far to buffer the route (in meters) for the search
  def queryRoute(route: Route): SimpleFeatureCollection // method that uses the route to find nearby feature instances

  def groupAndRank(skip: Int, max: Int, sortBy: String): ResultBean = {

    logger.info("Attempting Geomesa Route Rank on collection type " + dataFeatures.getClass.getName)
    if (!dataFeatures.isInstanceOf[AccumuloFeatureCollection]) {
      logger.warn("The provided data feature collection type may not support geomesa proximity search: "
        + dataFeatures.getClass.getName)
    }
    if (dataFeatures.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("Layer name in geoserver must match feature type name in geomesa")
    }

    val route = extractRoute
    val rv = route match {
      case Some(r) =>
        def getTimeAttrName(sfc: SimpleFeatureCollection) =
          index.getDtgDescriptor(sfc.getSchema).map{_.getLocalName}.getOrElse("geomesa_index_start_time")
        val spec = new SfSpec(keyField, getTimeAttrName(dataFeatures))
        val routeShape = r.route.bufferMeters(bufferMeters)
        // Find a square box that surrounds the route
        // We search:
        // 1. Along the route
        // 2. Within the box
        // This is so that features that occur frequently in the whole box can be assigned a lower ranking than features
        // that occur only along the route (e.g. for a wider context)
        val boxShape = boundingSquare(routeShape)
        val ff = CommonFactoryFinder.getFilterFactory2
        val boxFilter = ff.intersects(ff.property(dataFeatures.getSchema.getGeometryDescriptor.getLocalName),
          ff.literal(boxShape))
        val qp = new QueryProcess
        val routeSearchResults = queryRoute(r)
        val boxSearchResults = qp.execute(dataFeatures, boxFilter)
        val routeFeatures = new SimpleFeatureWithDateTimeAndKeyCollection(routeSearchResults, spec)
        val boxFeatures = new SimpleFeatureWithDateTimeAndKeyCollection(boxSearchResults, spec)
        val routeAndFeatures = new RouteAndSurroundingFeatures(r, boxFeatures, routeFeatures)
        routeAndFeatures.rank(boxShape.getEnvelopeInternal, List(routeShape))
      case _ =>
        logger.warn("Input feature to rank process must be a single LineString")
        Map[String, RankingValues]()
    }
    ResultBean.fromRankingValues(rv, sortBy, skip, max)
  }

  /**
   * Given a route geometry, computes a square box around it. Used for ranking context. See previous comment within the
   * group and rank method
   * @param bufferedRouteGeometry
   * @return geometry
   */
  private def boundingSquare(bufferedRouteGeometry: Geometry) = {
    val env1 = bufferedRouteGeometry.getEnvelopeInternal
    val diffLat = env1.getMaxY - env1.getMinY
    val diffLon = env1.getMaxX - env1.getMinX
    val centerLat = (env1.getMaxY + env1.getMinY) / 2.0
    val centerLon = (env1.getMaxX + env1.getMinX) / 2.0
    val delta = math.max(diffLat, diffLon) / 2.0
    val env2 = new ReferencedEnvelope(centerLon - delta, centerLon + delta, centerLat - delta, centerLat + delta,
      DefaultGeographicCRS.WGS84)
    JTS.toGeometry(env2)
  }

}

object FeatureGroupRanker {
  final val KEY_FIELD_NAME = "keyField"
  final val KEY_FIELD_DESCRIPTION = "The name of the key attribute to group by"

  final val SKIP_FIELD_NAME = "skip"
  final val SKIP_FIELD_DESCRIPTION = "The number of results to skip (for paging)"

  final val MAX_FIELD_NAME = "max"
  final val MAX_FIELD_DESCRIPTION = "The maximum number of results to return"

  final val SORT_BY_FIELD_NAME = "sortBy"
  final val SORT_BY_FIELD_DESCRIPTION = "The field to sort by, currently only \"combined.score\" is supported"
}