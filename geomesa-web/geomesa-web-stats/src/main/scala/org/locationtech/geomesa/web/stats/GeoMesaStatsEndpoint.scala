/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.web.stats

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.text.ecql.ECQL
import org.json4s.{DefaultFormats, Formats}
import org.locationtech.geomesa.utils.stats.MinMax
import org.locationtech.geomesa.web.core.GeoMesaServletCatalog.GeoMesaLayerInfo
import org.locationtech.geomesa.web.core.{GeoMesaScalatraServlet, GeoMesaServletCatalog}
import org.scalatra.BadRequest
import org.scalatra.json._

import scala.collection.JavaConversions._

class GeoMesaStatsEndpoint extends GeoMesaScalatraServlet with LazyLogging with NativeJsonSupport {
  override def root: String = "stats"

  override def defaultFormat: Symbol = 'json
  override protected implicit def jsonFormats: Formats = DefaultFormats

  logger.info("*** Starting the stats REST API endpoint!")

  before() {
    contentType = formats(format)
  }

  // Endpoint for debugging
  get("/:workspace/:layer/?") {
    retrieveInfo("get") match {
      case Some(statInfo) => logger.info("Found a GeoMesa datastore for the layer")
        val sft = statInfo.sft
        logger.info(s"SFT for layer is $sft.")

      case _ => logger.info("Didn't find a GeoMesa datastore.  Available ws/layer combinations are:")
        GeoMesaServletCatalog.getKeys.foreach { case (ws, layer) =>
          logger.info(s"Workspace: $ws Layer: $layer.")
        }
    }
  }

  // Returns an estimated count for the entire layer.
  get("/:workspace/:layer/count") {
    retrieveInfo("count") match {
      case Some(statInfo) =>
        val layer = params("layer")
        val sft = statInfo.sft

        logger.debug(s"Found a GeoMesa Accumulo datastore for $layer")
        logger.debug(s"SFT for $layer is $sft.")

        val geomesaStats = statInfo.ads.stats
        val countStat = params.get(GeoMesaStatsEndpoint.CqlFilterParam) match {
          case Some(filter) =>
            val cqlFilter = ECQL.toFilter(filter)
            logger.debug(s"Querying with filter: $filter")
            geomesaStats.getCount(sft, cqlFilter)
          case None => geomesaStats.getCount(sft)
        }

        countStat match {
          case Some(count) =>
            logger.debug(s"Retrieved count $count for $layer")
            count
          case None =>
            logger.debug(s"No estimated count for ${sft.getTypeName}")
            BadRequest(s"Estimated count for ${sft.getTypeName} is not available.")
        }

      case _ => BadRequest(s"No registered layer called ${params("layer")}")
    }
  }

  // Returns a spatial envelope for the layer.
  get("/:workspace/:layer/envelope") {
    retrieveInfo("envelope") match {
      case Some(statInfo) => logger.info("Found a GeoMesa datastore for the layer")
        val sft = statInfo.sft
        logger.info(s"SFT for layer is $sft.")

        statInfo.ads.stats.getBounds(sft)

      case _ => BadRequest(s"No registered layer called ${params("layer")}")
    }
  }

  // Gets the attribute bounds for a layer.
  get("/:workspace/:layer/bounds") {
    retrieveInfo("envelope") match {
      case Some(statInfo) => logger.info("Found a GeoMesa datastore for the layer")
        val sft = statInfo.sft
        val attrNames = sft.getAttributeDescriptors.map(_.getLocalName)

        val bounds = statInfo.ads.stats.getStats[MinMax[Any]](sft, attrNames)
        bounds.map(_.toJson).mkString("[", ",", "]")
      case _ => BadRequest(s"No registered layer called ${params("layer")}")
    }
  }

  def retrieveInfo(call: String): Option[GeoMesaLayerInfo] = {
    val workspace = params("workspace")
    val layer     = params("layer")
    logger.debug(s"Received $call request for workspace: $workspace and layer: $layer.")
    GeoMesaServletCatalog.getGeoMesaLayerInfo(workspace, layer)
  }
}

object GeoMesaStatsEndpoint {
  val CqlFilterParam = "cql_filter"
}


