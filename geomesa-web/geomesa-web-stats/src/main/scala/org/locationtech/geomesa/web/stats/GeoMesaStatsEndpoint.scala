/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.web.stats

import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.text.ecql.ECQL
import org.json4s.{DefaultFormats, Formats}
import org.locationtech.geomesa.tools.accumulo.commands.stats.StatsCommand
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.{Histogram, MinMax, Stat}
import org.locationtech.geomesa.web.core.GeoMesaServletCatalog.GeoMesaLayerInfo
import org.locationtech.geomesa.web.core.{GeoMesaScalatraServlet, GeoMesaServletCatalog}
import org.scalatra.BadRequest
import org.scalatra.json._
import org.scalatra.swagger._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class GeoMesaStatsEndpoint(val swagger: Swagger, rootPath: String = GeoMesaScalatraServlet.DefaultRootPath)
    extends GeoMesaScalatraServlet with LazyLogging with NativeJsonSupport with SwaggerSupport {

  // must override applicationName for Swagger to work
  override def applicationName: Option[String] = Some(s"$rootPath/$root")
  override def root: String = "stats"
  override def defaultFormat: Symbol = 'json
  override protected def applicationDescription: String = "The GeoMesa Stats API"
  override protected implicit def jsonFormats: Formats = DefaultFormats

  // GeoServer's AdvancedDispatcherFilter tries to help us out, but it gets in the way.
  //  For our purposes, we want to unwrap those filters.
  // CorsSupport and other Scalatra classes/traits override handle, and will choke on GS's ADF:(
  // To fix this, we need to have this unwrapping happen.
  //  We can achieve that in one of two ways:
  //  1.  Put the below override in this class.
  //  2.  Make a trait which has this override in and make sure it appears last (or merely latter than CorsSupport, etc.)
  override def handle(req: HttpServletRequest, res: HttpServletResponse): Unit = req match {
    case r: HttpServletRequestWrapper => super.handle(r.getRequest.asInstanceOf[HttpServletRequest], res)
    case _ => super.handle(req, res)
  }

  logger.info("*** Starting the stats REST API endpoint!")

  before() {
    contentType = formats(format)
  }

  val getCount = (
        apiOperation[Integer]("getCount")
        summary "Gets an estimated count of simple features"
        notes "Gets an estimated count of simple features from the stats table in Accumulo."
        parameters (
        pathParam[String]("workspace").description("GeoServer workspace."),
        pathParam[String]("layer").description("GeoServer layer."),
        queryParam[Option[String]]("cql_filter").description("A CQL filter to compute the count of simple features against. If omitted, the CQL filter will be Filter.INCLUDE."))
    )

  get("/:workspace/:layer/count", operation(getCount)) {
    retrieveLayerInfo("count") match {
      case Some(statInfo) =>
        val layer = params(GeoMesaStatsEndpoint.LayerParam)
        val sft = statInfo.sft

        logger.debug(s"Found a GeoMesa Accumulo datastore for $layer")
        logger.debug(s"SFT for $layer is ${SimpleFeatureTypes.encodeType(sft)}")

        // obtain stats using cql filter if present
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
            BadRequest(s"Estimated count for ${sft.getTypeName} is not available")
        }

      case _ => BadRequest(s"No registered layer called ${params(GeoMesaStatsEndpoint.LayerParam)}")
    }
  }

  val getBounds = (
        apiOperation[String]("getBounds")
        summary "Gets the bounds of attributes"
        notes "Gets the bounds of attributes from the stats table in Accumulo."
        parameters (
        pathParam[String]("workspace").description("GeoServer workspace."),
        pathParam[String]("layer").description("GeoServer layer."),
        queryParam[Option[String]]("attributes").description("A comma separated list of attribute names to retrieve bounds for. If omitted, all attributes will be used."))
    )

  get("/:workspace/:layer/bounds", operation(getBounds)) {
    retrieveLayerInfo("bounds") match {
      case Some(statInfo) =>
        val layer = params(GeoMesaStatsEndpoint.LayerParam)
        val sft = statInfo.sft

        logger.debug(s"Found a GeoMesa Accumulo datastore for $layer")
        logger.debug(s"SFT for $layer is ${SimpleFeatureTypes.encodeType(sft)}")

        val userAttributes: Seq[String] = params.get(GeoMesaStatsEndpoint.AttributesParam) match {
          case Some(attributesString) => attributesString.split(',')
          case None => Nil
        }
        val attributes = StatsCommand.getAttributes(sft, userAttributes)

        val boundStatList = statInfo.ads.stats.getStats[MinMax[Any]](sft, attributes)

        val jsonBoundsList = attributes.map { attribute =>
          val i = sft.indexOf(attribute)
          val out = boundStatList.find(_.attribute == i) match {
            case None => "\"unavailable\""
            case Some(mm) if mm.isEmpty => "\"no matching data\""
            case Some(mm) => mm.toJson
          }

          s""""$attribute": $out"""
        }

        jsonBoundsList.mkString("{", ", ", "}")

      case _ => BadRequest(s"No registered layer called ${params(GeoMesaStatsEndpoint.LayerParam)}")
    }
  }

  val getHistograms = (
        apiOperation[String]("getHistogram")
        summary "Gets histograms of attributes"
        notes "Gets histograms of attributes from the stats table in Accumulo."
        parameters (
        pathParam[String]("workspace").description("GeoServer workspace."),
        pathParam[String]("layer").description("GeoServer layer."),
        queryParam[Option[String]]("attributes").description("A comma separated list of attribute names to retrieve bounds for. If omitted, all attributes will be used."),
        queryParam[Option[Integer]]("bins").description("The number of bins the histograms will have. Defaults to 1000."))
    )

  get("/:workspace/:layer/histogram", operation(getHistograms)) {
    retrieveLayerInfo("histogram") match {
      case Some(statInfo) =>
        val layer = params(GeoMesaStatsEndpoint.LayerParam)
        val sft = statInfo.sft

        logger.debug(s"Found a GeoMesa Accumulo datastore for $layer")
        logger.debug(s"SFT for $layer is ${SimpleFeatureTypes.encodeType(sft)}")

        val userAttributes: Seq[String] = params.get(GeoMesaStatsEndpoint.AttributesParam) match {
          case Some(attributesString) => attributesString.split(',')
          case None => Nil
        }
        val attributes = StatsCommand.getAttributes(sft, userAttributes)

        val bins = params.get(GeoMesaStatsEndpoint.BinsParam).map(_.toInt)

        val histograms = statInfo.ads.stats.getStats[Histogram[Any]](sft, attributes).map {
          case histogram: Histogram[Any] if bins.forall(_ == histogram.length) => histogram
          case histogram: Histogram[Any] =>
            val descriptor = sft.getDescriptor(histogram.attribute)
            val ct = ClassTag[Any](descriptor.getType.getBinding)
            val attribute = descriptor.getLocalName
            val statString = Stat.Histogram[Any](attribute, bins.get, histogram.min, histogram.max)(ct)
            val binned = Stat(sft, statString).asInstanceOf[Histogram[Any]]
            binned.addCountsFrom(histogram)
            binned
        }

        val jsonHistogramList = attributes.map { attribute =>
          val out = histograms.find(_.attribute == sft.indexOf(attribute)) match {
            case None => "\"unavailable\""
            case Some(hist) if hist.isEmpty => "\"no matching data\""
            case Some(hist) => hist.toJson
          }

          s""""$attribute": $out"""
        }

        jsonHistogramList.mkString("{", ", ", "}")

      case _ => BadRequest(s"No registered layer called ${params(GeoMesaStatsEndpoint.LayerParam)}")
    }
  }

  def retrieveLayerInfo(call: String): Option[GeoMesaLayerInfo] = {
    val workspace = params(GeoMesaStatsEndpoint.WorkspaceParam)
    val layer     = params(GeoMesaStatsEndpoint.LayerParam)
    logger.debug(s"Received $call request for workspace: $workspace and layer: $layer.")
    GeoMesaServletCatalog.getGeoMesaLayerInfo(workspace, layer)
  }
}

object GeoMesaStatsEndpoint {
  val LayerParam = "layer"
  val WorkspaceParam = "workspace"
  val CqlFilterParam = "cql_filter"
  val AttributesParam = "attributes"
  val BinsParam = "bins"
}


