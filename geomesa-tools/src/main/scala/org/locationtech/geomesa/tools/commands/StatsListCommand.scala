/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.commands

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaMetadataStats
import org.locationtech.geomesa.tools.commands.StatsListCommand.StatsListParameters
import org.locationtech.geomesa.utils.stats.{RangeHistogram, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

class StatsListCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  import StatsListCommand.{createStatString, geomHistToString, getAttributes, histToString}
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val command = "stats-list"
  override val params = new StatsListParameters

  override def execute() = {
    val sft = ds.getSchema(params.featureName)
    val attributes = getAttributes(sft, params)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter)
    val bins = Option(params.bins).map(_.intValue)

    val histograms = if (params.exact) {
      attributes.map(a => (a, getExactStat(sft, a, filter.getOrElse(Filter.INCLUDE), bins)))
    } else {
      filter.foreach(_ => logger.warn("Ignoring CQL filter for non-exact stat query"))
      attributes.map(a => (a, getExistingStat(sft, a, bins)))
    }
    histograms.foreach {
      case (attr, None) => System.err.println(s"No stats available for attribute '$attr'")
      case (attr, Some(hist)) =>
        if (attr == sft.getGeomField) {
          println(geomHistToString(attr, hist.asInstanceOf[RangeHistogram[Geometry]]))
        } else {
          println(histToString(hist, sft, attr))
        }
    }
  }

  private def getExistingStat(sft: SimpleFeatureType,
                              attribute: String,
                              bins: Option[Int]): Option[RangeHistogram[Any]] = {
    ds.stats.getHistogram[Any](sft, attribute).map { histogram =>
      bins match {
        case None => histogram
        case Some(b) if b == histogram.length => histogram
        case Some(b) =>
          val statString = createStatString(attribute, histogram.bounds, b)
          val binned = Stat(sft, statString).asInstanceOf[RangeHistogram[Any]]
          binned.addCountsFrom(histogram)
          binned
      }
    }
  }

  private def getExactStat(sft: SimpleFeatureType,
                           attribute: String,
                           filter: Filter,
                           bins: Option[Int]): Option[RangeHistogram[Any]] = {
    import GeoMesaMetadataStats.{DefaultHistogramSize => DefaultSize, GeometryHistogramSize => GeomSize}
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    System.err.println(s"Calculating stats for '$attribute'...")

    def getEndpoints(exact: Boolean) = ds.stats.getMinMax[Any](sft, attribute, filter, exact = exact)

    // determine endpoints - use cached if available, else do a query
    // if endpoints are equal, we can't create a histogram (and it would only have one bucket anyway...)
    getEndpoints(false).orElse(getEndpoints(true)).filter { case (l, r) => l != r }.map { bounds =>
      val length = bins.getOrElse(if (sft.getGeomField == attribute) GeomSize else DefaultSize)
      val statString = createStatString(attribute, bounds, length)
      ds.stats.runStatQuery[RangeHistogram[Any]](sft, statString, filter)
    }
  }
}

object StatsListCommand {

  @Parameters(commandDescription = "View statistics on a GeoMesa feature type")
  class StatsListParameters extends OptionalCqlFilterParameters with HasAttributesParam {
    @Parameter(names = Array("-e", "--exact"), description = "Calculate exact statistics (may be slow)")
    var exact: Boolean = false

    @Parameter(names = Array("-b", "--bins"), description = "How many buckets the data will be divided into")
    var bins: Integer = null
  }

  // note: we need the class type in order to create the bounds string
  def createStatString(attribute: String, bounds: (Any, Any), length: Int): String = {
    bounds match {
      case (min: String,  max: String)    => Stat.RangeHistogram(attribute, length, min, max)
      case (min: Integer, max: Integer)   => Stat.RangeHistogram(attribute, length, min, max)
      case (min: jLong,   max: jLong)     => Stat.RangeHistogram(attribute, length, min, max)
      case (min: jFloat,  max: jFloat)    => Stat.RangeHistogram(attribute, length, min, max)
      case (min: jDouble, max: jDouble)   => Stat.RangeHistogram(attribute, length, min, max)
      case (min: Geometry, max: Geometry) => Stat.RangeHistogram(attribute, length, min, max)
      case (min: Date, max: Date)         => Stat.RangeHistogram(attribute, length, min, max)
    }
  }

  /**
    * Creates a readable string for the histogram.
    */
  def histToString(stat: RangeHistogram[Any], sft: SimpleFeatureType, attribute: String): String = {
    val counts = (0 until stat.length).toList.flatMap { i =>
      val count = stat.count(i)
      if (count < 1) { None } else {
        val (min, max) = stat.bounds(i)
        Some(s"[ ${stat.stringify(min)} to ${stat.stringify(max)} ] $count")
      }
    }
    s"Binned histogram for '$attribute' (${sft.getDescriptor(attribute).getType.getBinding.getSimpleName}): " +
        s"'${stat.stringify(stat.min)}' to '${stat.stringify(stat.max)}'\n  ${counts.mkString("\n  ")}"
  }

  /**
    * Creates string containing an ASCII, color-coded map of densities.
    */
  def geomHistToString(attribute: String, stat: RangeHistogram[Geometry]): String = {
    // grid of counts, corresponds to our world map dimensions
    val counts = Array.fill[Array[Long]](AsciiWorldMapHeight)(Array.fill[Long](AsciiWorldMapLength)(0))
    // min/max to normalize our densities
    var min = stat.count(0)
    var max = min

    // translate histogram values into the grid and also calculate min/max for normalization
    def putCountsInGrid(): Unit = {
      var i = 0
      while (i < stat.length) {
        val count = stat.count(i)
        if (count > max) {
          max = count
        } else if (count < min) {
          min = count
        }
        if (count > 0) {
          val point = stat.medianValue(i).asInstanceOf[Point]
          val (x, y) = (point.getX, point.getY)
          val xOffset = (x + 180) / 360 // normalize to 0-1
          val yOffset = 1 - (y + 90) / 180  // normalize to 0-1 and invert axis
          // index into map string
          val j = math.round(yOffset * AsciiWorldMapHeight).toInt
          val k = math.round(xOffset * AsciiWorldMapLength).toInt
          counts(j)(k) += count
        }
        i += 1
      }
    }

    // normalize a count to 0-1 based on our min/max values
    def normalize(count: Long): Float = (count - min).toFloat / (max - min)
    // reverse a normalized percent
    def denormalize(percent: Float): Long = (percent * (max - min)).toLong + min

    putCountsInGrid()

    val sb = new StringBuilder

    // build up our string - if we have data in a given cell, put a sized circle, else put the map char
    // in addition to size of circle, use console ANSI colors to mark our densities
    var i = 0
    var currentColor: String = null
    while (i < AsciiWorldMapHeight) {
      val row = counts(i)
      var j = 0
      while (j < AsciiWorldMapLength) {
        val normalized = normalize(row(j))
        val (color, char) =
          if (normalized < .1f) {
            (Console.RESET, AsciiWorldMap(i)(j))
          } else if (normalized < .3f) {
            Threshold1
          } else if (normalized < .5f) {
            Threshold2
          } else if (normalized < .7f) {
            Threshold3
          } else {
            Threshold4
          }
        if (color != currentColor) {
          sb.append(color)
          currentColor = color
        }
        sb.append(char)
        j += 1
      }
      sb.append('\n')
      i += 1
    }

    // write out a key of the actual count ranges represented by each marker
    sb.append(s"\nKey: [${Threshold1._1}${Threshold1._2}${Console.RESET} ${denormalize(0.1f)} to ${denormalize(0.3f)}] ")
    sb.append(s"[${Threshold2._1}${Threshold2._2}${Console.RESET} ${denormalize(0.3f)} to ${denormalize(0.5f)}] ")
    sb.append(s"[${Threshold3._1}${Threshold3._2}${Console.RESET} ${denormalize(0.5f)} to ${denormalize(0.8f)}] ")
    sb.append(s"[${Threshold4._1}${Threshold4._2}${Console.RESET}  ${denormalize(0.8f)} to $max]")

    sb.toString
  }

  // gets attributes to run stats on, based on sft and input params
  def getAttributes(sft: SimpleFeatureType, params: StatsListParameters): Seq[String] = {
    val allAttributes: Seq[String] = Option(params.attributes).map(_.split(",")) match {
      case None => sft.getAttributeDescriptors.map(_.getLocalName)
      case Some(a) => a.map(_.trim)
    }
    allAttributes.filter { a =>
      val ad = sft.getDescriptor(a)
      if (GeoMesaMetadataStats.okForStats(ad)) { true } else {
        System.err.println(s"Ignoring attribute '$a' of unsupported type ${ad.getType.getBinding.getSimpleName}")
        false
      }
    }
  }

  // ANSI colors and unicode values for various density counts
  private val Threshold1 = (Console.CYAN + Console.BOLD, '\u26AB')   // 26AB - black circle
  private val Threshold2 = (Console.GREEN + Console.BOLD, '\u25CF')  // 25CF - medium black circle
  private val Threshold3 = (Console.YELLOW + Console.BOLD, '\u25CF') // 25CF - medium black circlt
  private val Threshold4 = (Console.RED + Console.BOLD, '\u2B24')    // 2B24 - black circle large

  // note: keep these height and length variables consistent with the map
  private val AsciiWorldMapLength = 140
  private val AsciiWorldMapHeight = 42

  // map is spaced so that points project semi-accurately - see @StatsListCommandTest for test cases
  private val AsciiWorldMap = Array(
    """                                                                                                                                            """,
    """                                                                                                                                            """,
    """                                          .      .   ,:,,:                                                                                  """,
    """                                    . :,.:,,,::,.,,,.,__,__,,.,;                                    :.                                      """,
    """                              ,. ;:,__:,.,,,,.,,____________,,          :,,:                          __                                    """,
    """                           ,.,::::,  .::;    .  ,,____________.                         ,,.        ;.,.,,__        ,;.                      """,
    """                          .,,:::;:,:: .,, .      ,__________,                         ,    .,. ,,________.,.,,__    ::                      """,
    """             ______,: :,,,/; ,.,,; ,;  ,. ,,,.    :______,,.,             ,____;   __    , __.,__________________________.:;;;.,,           """,
    """            ,,____________; ________.,,    .,,,.  __.,./.               ,.,,;__. ;,,,______.;__________________________________,,:,.,       """,
    """            ,,__.,.,________.:,__,.,,   .,,, ,     :.,.               ,,,__:__.,:______________________________________.,,.,.,__;           """,
    """             .:,,    .,,______;,,__.:     ____,                       ,,.,  ;,__.,________________________________.,      :,                """,
    """            ;          ,__________,,__,,.:,____,,                   .  ,.,,,______.,:__________________.,:________\__    ,,                 """,
    """                         ,,,________,.:__.,.,,:: ,                 .:,________________________________,,,__________,,                       """,
    """                          __________.,.: .,__,/:                    __.,,,____,:;__.:,.,;__.,:,__________________,, :                       """,
    """                          .,__________,__.,                      ,,.:  : , .::;.,:.,__:,.,__________________,:__   __                       """,
    """                           ,,,__________.,,                       :,,__.      :,;__________________________,,  ,.:,,                        """,
    """                             :,____.,.,,:                        ______.,.:____,,__,,,____________________.,,,                              """,
    """                              ,,,.,                            ,,________________.,__:.: ,,,______,________.,;                              """,
    """                                 ,,, :,    ;:.                 ________________,. ,__,,,     ,.,,.  ,,__ :                                  """,
    """                                     ,:.                      ,________.,,______.,,,,        ,.,     ,,__.   :,                             """,
    """                                        , .,,,,,,               ,________________.,,,         ,:      ,       ,                             """,
    """                                         ,,____.,,.,                   ,__________.:                 .,; :,,. ,:                            """,
    """                                         __________.,__:                ,,____,,.,                     .   __: . :,__,                      """,
    """                                          ,,____________                 ________,                          . :     . :.                    """,
    """                                           ,,,________.                 __________ ,,.                        :__., ,,                      """,
    """                                            :________,:                  ,____.,.  .:                      ,__________,     ,.              """,
    """                                            ,,____.,                     ,____,,                           ,__________,,                    """,
    """                                            ____.,:                       :__,                             ;.,.:__,____,                    """,
    """                                           ,____.                                                                  .,,.        :;.          """,
    """                                           ;__/                                                                      ,,       .             """,
    """                                           ,__                                                                                              """,
    """                                           ,,                                                                                               """,
    """                                                                                                                                            """,
    """                                                                                                                                            """,
    """                                          /                                                                                                 """,
    """                                       ____                 _ _/:\_____/________________ ____________________________________.              """,
    """          :.,.________________________.              ______________________________________________________________________                 """,
    """          .________________________________ .___/____________________________________________________________________________,              """,
    """  ________________________________________________________________________________________________________________________________          """,
    """                                                                                                                                            """,
    """                                                                                                                                            """,
    """                                                                                                                                            """,
    """                                                                                                                                            """
  ).map(_.toCharArray)
}
