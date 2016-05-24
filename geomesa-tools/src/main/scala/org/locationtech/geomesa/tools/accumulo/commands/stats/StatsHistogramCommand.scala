/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands.stats

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.tools.accumulo.commands.CommandWithCatalog
import org.locationtech.geomesa.tools.accumulo.commands.stats.StatsHistogramCommand.StatsHistogramParams
import org.locationtech.geomesa.utils.stats.{MinMax, RangeHistogram, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.reflect.ClassTag

class StatsHistogramCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  import StatsHistogramCommand.{geomHistToString, histToString}

  override val command = "stats-histogram"
  override val params = new StatsHistogramParams

  override def execute() = {
    val sft = ds.getSchema(params.featureName)
    val attributes = StatsCommand.getAttributes(sft, params)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val bins = Option(params.bins).map(_.intValue)

    val histograms = if (params.exact) {
      logger.info("Running stat query...")
      val query = Stat.SeqStat(attributes.map { attribute =>
        val ct = ClassTag[Any](sft.getDescriptor(attribute).getType.getBinding)
        val mm = ds.stats.getStats[MinMax[Any]](sft, Seq(attribute)).headOption
        val bounds = mm match {
          case None => GeoMesaStats.defaultBounds(ct.runtimeClass)
          case Some(b) if b.min == b.max => RangeHistogram.buffer(b.min)
          case Some(b) => b.bounds
        }
        val length = bins.getOrElse(GeoMesaStats.DefaultHistogramSize)
        Stat.RangeHistogram[Any](attribute, length, bounds._1, bounds._2)(ct)
      })
      ds.stats.runStats[RangeHistogram[Any]](sft, query, filter)
    } else {
      if (filter != Filter.INCLUDE) {
        logger.warn("Ignoring CQL filter for non-exact stat query")
      }
      ds.stats.getStats[RangeHistogram[Any]](sft, attributes).map {
        case histogram: RangeHistogram[Any] if bins.forall(_ == histogram.length) => histogram
        case histogram: RangeHistogram[Any] =>
          val descriptor = sft.getDescriptor(histogram.attribute)
          val ct = ClassTag[Any](descriptor.getType.getBinding)
          val attribute = descriptor.getLocalName
          val statString = Stat.RangeHistogram[Any](attribute, bins.get, histogram.min, histogram.max)(ct)
          val binned = Stat(sft, statString).asInstanceOf[RangeHistogram[Any]]
          binned.addCountsFrom(histogram)
          binned
      }
    }

    attributes.foreach { attribute =>
      histograms.find(_.attribute == sft.indexOf(attribute)) match {
        case None => logger.info(s"No histogram available for attribute '$attribute'")
        case Some(hist) =>
          if (classOf[Geometry].isAssignableFrom(sft.getDescriptor(attribute).getType.getBinding)) {
            println(geomHistToString(attribute, hist.asInstanceOf[RangeHistogram[Geometry]]))
          } else {
            println(histToString(hist, sft, attribute))
          }
      }
    }
  }
}

object StatsHistogramCommand {

  @Parameters(commandDescription = "View statistics on a GeoMesa feature type")
  class StatsHistogramParams extends StatsWithAttributeParams {
    @Parameter(names = Array("-b", "--bins"), description = "How many bins the data will be divided into. " +
        "For example, if you are examining a week of data, you may want to divide the date into 7 bins, one per day.")
    var bins: Integer = null
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
    s"Binned histogram for '$attribute':${if (counts.isEmpty) " No values" else counts.mkString("\n  ", "\n  ", "")}"
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
          val j = math.floor(yOffset * AsciiWorldMapHeight).toInt
          val k = math.floor(xOffset * AsciiWorldMapLength).toInt
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
          } else if (normalized < .8f) {
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
    sb.append(s"${Console.RESET}\nKey: ")
    sb.append(s"[${Threshold1._1}${Threshold1._2}${Console.RESET} ${denormalize(0.1f)} to ${denormalize(0.3f)}] ")
    sb.append(s"[${Threshold2._1}${Threshold2._2}${Console.RESET} ${denormalize(0.3f)} to ${denormalize(0.5f)}] ")
    sb.append(s"[${Threshold3._1}${Threshold3._2}${Console.RESET} ${denormalize(0.5f)} to ${denormalize(0.8f)}] ")
    sb.append(s"[${Threshold4._1}${Threshold4._2}${Console.RESET}  ${denormalize(0.8f)} to $max]")

    sb.toString
  }

  // ANSI colors and unicode values for various density counts
  private val Threshold1 = (Console.BOLD + Console.CYAN,   '\u26AB') // 26AB - black circle
  private val Threshold2 = (Console.BOLD + Console.GREEN,  '\u25CF') // 25CF - medium black circle
  private val Threshold3 = (Console.BOLD + Console.YELLOW, '\u25CF') // 25CF - medium black circle
  private val Threshold4 = (Console.BOLD + Console.RED,    '\u2B24') // 2B24 - black circle large

  // note: keep these height and length variables consistent with the map
  private val AsciiWorldMapLength = 140
  private val AsciiWorldMapHeight = 42

  // map is spaced so that points project semi-accurately - see @StatsHistogramCommandTest for test cases
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
