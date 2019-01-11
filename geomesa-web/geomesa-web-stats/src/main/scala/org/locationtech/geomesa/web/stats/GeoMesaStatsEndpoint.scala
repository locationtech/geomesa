/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.web.stats

import com.typesafe.scalalogging.LazyLogging
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.geotools.filter.text.ecql.ECQL
import org.json4s.{DefaultFormats, Formats}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.tools.stats.StatsCommand
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.{Histogram, MinMax, Stat}
import org.locationtech.geomesa.web.core.GeoMesaServletCatalog.GeoMesaLayerInfo
import org.locationtech.geomesa.web.core.{GeoMesaScalatraServlet, GeoMesaServletCatalog}
import org.opengis.filter.Filter
import org.scalatra.BadRequest
import org.scalatra.json._
import org.scalatra.swagger._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class GeoMesaStatsEndpoint(val swagger: Swagger, rootPath: String = GeoMesaScalatraServlet.DefaultRootPath)
    extends GeoMesaScalatraServlet with LazyLogging with NativeJsonSupport with SwaggerSupport {

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
  override def handle(req: HttpServletRequest, res: HttpServletResponse): Unit =
    super.handle(GeoMesaScalatraServlet.wrap(req), res)

  logger.info("*** Starting the stats REST API endpoint!")

  before() {
    contentType = formats(format)
  }

  val getCount = (
        apiOperation[Integer]("getCount")
        summary "Gets an estimated count of simple features"
        notes "Gets an estimated count of simple features from the stats table in Accumulo."
        parameters (
        pathParam[String](GeoMesaStatsEndpoint.WorkspaceParam).description("GeoServer workspace."),
        pathParam[String](GeoMesaStatsEndpoint.LayerParam).description("GeoServer layer."),
        queryParam[Option[String]](GeoMesaStatsEndpoint.CqlFilterParam).description("A CQL filter to compute the count of simple features against. Defaults to Filter.INCLUDE."),
        queryParam[Option[Boolean]](GeoMesaStatsEndpoint.NoCacheParam).description("Calculate stats against the data set instead of using cached statistics (may be slow)."))
    )

  get("/:workspace/:layer/count", operation(getCount)) {
    retrieveLayerInfo("count") match {
      case Some(statInfo) =>
        val layer = params(GeoMesaStatsEndpoint.LayerParam)
        val sft = statInfo.sft
        val stats = statInfo.ds.asInstanceOf[HasGeoMesaStats].stats
        val filter = params.get(GeoMesaStatsEndpoint.CqlFilterParam).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
        val noCache = params.get(GeoMesaStatsEndpoint.NoCacheParam).exists(_.toBoolean)

        logger.debug(s"Found a GeoMesa Accumulo datastore for $layer")
        logger.debug(s"SFT for $layer is ${SimpleFeatureTypes.encodeType(sft)}")
        logger.debug(s"Running stat with filter: $filter")
        logger.debug(s"Running stat with no cached stats: $noCache")

        val countStat = stats.getCount(sft, filter, noCache)

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
        pathParam[String](GeoMesaStatsEndpoint.WorkspaceParam).description("GeoServer workspace."),
        pathParam[String](GeoMesaStatsEndpoint.LayerParam).description("GeoServer layer."),
        queryParam[Option[String]](GeoMesaStatsEndpoint.AttributesParam).description("A comma separated list of attribute names to retrieve bounds for. If omitted, all attributes will be used."),
        queryParam[Option[String]](GeoMesaStatsEndpoint.CqlFilterParam).description("A CQL filter to compute the count of simple features against. Defaults to Filter.INCLUDE. Will not be used if the noCache parameter is false."),
        queryParam[Option[Boolean]](GeoMesaStatsEndpoint.NoCacheParam).description("Calculate stats against the data set instead of using cached statistics (may be slow)."))
    )

  get("/:workspace/:layer/bounds", operation(getBounds)) {
    retrieveLayerInfo("bounds") match {
      case Some(statInfo) =>
        val layer = params(GeoMesaStatsEndpoint.LayerParam)
        val sft = statInfo.sft
        val stats = statInfo.ds.asInstanceOf[HasGeoMesaStats].stats
        val filter = params.get(GeoMesaStatsEndpoint.CqlFilterParam).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
        val noCache = params.get(GeoMesaStatsEndpoint.NoCacheParam).exists(_.toBoolean)

        logger.debug(s"Found a GeoMesa Accumulo datastore for $layer")
        logger.debug(s"SFT for $layer is ${SimpleFeatureTypes.encodeType(sft)}")
        logger.debug(s"Running stat with filter: $filter")
        logger.debug(s"Running stat with no cached stats: $noCache")

        val userAttributes: Seq[String] = params.get(GeoMesaStatsEndpoint.AttributesParam) match {
          case Some(attributesString) => attributesString.split(',')
          case None => Nil
        }
        val attributes = StatsCommand.getAttributes(sft, userAttributes)

        val boundStatList = if (noCache) {
          val statQuery = Stat.SeqStat(attributes.map(Stat.MinMax))
          stats.runStats[MinMax[Any]](sft, statQuery, filter)
        } else {
          stats.getStats[MinMax[Any]](sft, attributes)
        }

        val jsonBoundsList = attributes.map { attribute =>
          val out = boundStatList.find(_.property == attribute) match {
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
        pathParam[String](GeoMesaStatsEndpoint.WorkspaceParam).description("GeoServer workspace."),
        pathParam[String](GeoMesaStatsEndpoint.LayerParam).description("GeoServer layer."),
        queryParam[Option[String]](GeoMesaStatsEndpoint.AttributesParam).description("A comma separated list of attribute names to retrieve bounds for. If omitted, all attributes will be used."),
        queryParam[Option[Integer]](GeoMesaStatsEndpoint.BinsParam).description("The number of bins the histograms will have. Defaults to 1000."),
        queryParam[Option[String]](GeoMesaStatsEndpoint.CqlFilterParam).description("A CQL filter to compute the count of simple features against. Defaults to Filter.INCLUDE. Will not be used if the noCache parameter is false."),
        queryParam[Option[Boolean]](GeoMesaStatsEndpoint.NoCacheParam).description("Calculate stats against the data set instead of using cached statistics (may be slow)."),
        queryParam[Option[Boolean]](GeoMesaStatsEndpoint.CalculateBoundsParam).description("Calculates the bounds of each histogram. Will use the default bounds if false. Will not be used if the noCache parameter is false."))
    )

  get("/:workspace/:layer/histogram", operation(getHistograms)) {
    retrieveLayerInfo("histogram") match {
      case Some(statInfo) =>
        val layer = params(GeoMesaStatsEndpoint.LayerParam)
        val sft = statInfo.sft
        val stats = statInfo.ds.asInstanceOf[HasGeoMesaStats].stats
        val filter = params.get(GeoMesaStatsEndpoint.CqlFilterParam).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
        val noCache = params.get(GeoMesaStatsEndpoint.NoCacheParam).exists(_.toBoolean)
        val calculateBounds = params.get(GeoMesaStatsEndpoint.CalculateBoundsParam).exists(_.toBoolean)

        logger.debug(s"Found a GeoMesa Accumulo datastore for $layer")
        logger.debug(s"SFT for $layer is ${SimpleFeatureTypes.encodeType(sft)}")
        logger.debug(s"Running stat with filter: $filter")
        logger.debug(s"Running stat with no cached stats: $noCache")

        val userAttributes: Seq[String] = params.get(GeoMesaStatsEndpoint.AttributesParam) match {
          case Some(attributesString) => attributesString.split(',')
          case None => Nil
        }
        val attributes = StatsCommand.getAttributes(sft, userAttributes)

        val bins = params.get(GeoMesaStatsEndpoint.BinsParam).map(_.toInt)

        val histograms = if (noCache) {
          val bounds = scala.collection.mutable.Map.empty[String, (Any, Any)]
          attributes.foreach { attribute =>
            stats.getStats[MinMax[Any]](sft, Seq(attribute)).headOption.foreach { b =>
              bounds.put(attribute, if (b.min == b.max) Histogram.buffer(b.min) else b.bounds)
            }
          }

          if (bounds.size != attributes.size) {
            val noBounds = attributes.filterNot(bounds.contains)
            logger.warn(s"Initial bounds are not available for attributes ${noBounds.mkString(", ")}.")

            if (calculateBounds) {
              logger.debug("Calculating bounds...")
              stats.runStats[MinMax[Any]](sft, Stat.SeqStat(noBounds.map(Stat.MinMax)), filter).foreach { mm =>
                bounds.put(mm.property, mm.bounds)
              }
            } else {
              logger.debug("Using default bounds.")
              noBounds.foreach { attribute =>
                val ct = ClassTag[Any](sft.getDescriptor(attribute).getType.getBinding)
                bounds.put(attribute, GeoMesaStats.defaultBounds(ct.runtimeClass))
              }
            }
          }

          logger.debug("Running live histogram stat query...")
          val length = bins.getOrElse(GeoMesaStats.DefaultHistogramSize)
          val queries = attributes.map { attribute =>
            val ct = ClassTag[Any](sft.getDescriptor(attribute).getType.getBinding)
            val (lower, upper) = bounds(attribute)
            Stat.Histogram[Any](attribute, length, lower, upper)(ct)
          }
          stats.runStats[Histogram[Any]](sft, Stat.SeqStat(queries), filter)
        } else {
          stats.getStats[Histogram[Any]](sft, attributes).map {
            case histogram: Histogram[Any] if bins.forall(_ == histogram.length) => histogram
            case histogram: Histogram[Any] =>
              val descriptor = sft.getDescriptor(histogram.property)
              val ct = ClassTag[Any](descriptor.getType.getBinding)
              val statString = Stat.Histogram[Any](histogram.property, bins.get, histogram.min, histogram.max)(ct)
              val binned = Stat(sft, statString).asInstanceOf[Histogram[Any]]
              binned.addCountsFrom(histogram)
              binned
          }
        }

        val jsonHistogramList = attributes.map { attribute =>
          val out = histograms.find(_.property == attribute) match {
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
  val NoCacheParam = "noCache"
  val CalculateBoundsParam = "calculateBounds"
}
