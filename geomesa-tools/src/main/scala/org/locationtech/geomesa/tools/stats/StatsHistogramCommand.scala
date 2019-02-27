/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import com.beust.jcommander.Parameter
import org.geotools.data.DataStore
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.stats.{Histogram, MinMax, Stat}
import org.locationtech.jts.geom.{Geometry, Point}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.reflect.ClassTag
import scala.util.Try

trait StatsHistogramCommand[DS <: DataStore with HasGeoMesaStats] extends DataStoreCommand[DS] {

  override val name = "stats-histogram"
  override def params: StatsHistogramParams

  override def execute(): Unit = withDataStore(histogram)

  protected def histogram(ds: DS): Unit = {
    val sft = ds.getSchema(params.featureName)
    val attributes = StatsCommand.getAttributesFromParams(sft, params)
    val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)
    val bins = Option(params.bins).map(_.intValue)

    val histograms = if (params.exact) {
      val bounds = scala.collection.mutable.Map.empty[String, (Any, Any)]
      attributes.foreach { attribute =>
        ds.stats.getStats[MinMax[Any]](sft, Seq(attribute)).headOption.foreach { b =>
          bounds.put(attribute, if (b.min == b.max) Histogram.buffer(b.min) else b.bounds)
        }
      }

      if (bounds.size != attributes.size) {
        val noBounds = attributes.filterNot(bounds.contains)
        Command.user.warn(s"Initial bounds are not available for attributes ${noBounds.mkString(", ")}.")
        var response: Integer = null
        Command.user.info("Would you like to:\n" +
                "  1. Calculate bounds (may be slow)\n" +
                "  2. Use default bounds (may be less accurate)\n" +
                "  3. Manually enter bounds\n" +
                "  4. Cancel operation\n")
        while (response == null) {
          val in = Prompt.read("Please enter the number of your choice: ")
          response = Try(in.toInt.asInstanceOf[Integer]).filter(r => r > 0 && r < 5).getOrElse(null)
          if (response == null) {
            Command.user.error("Invalid input. Please enter 1-4.")
          }
        }
        if (response == 1) {
          Command.user.info("Running bounds query...")
          ds.stats.runStats[MinMax[Any]](sft, Stat.SeqStat(noBounds.map(Stat.MinMax)), filter).foreach { mm =>
            bounds.put(mm.property, mm.bounds)
          }
        } else if (response == 2) {
          noBounds.foreach { attribute =>
            val ct = ClassTag[Any](sft.getDescriptor(attribute).getType.getBinding)
            bounds.put(attribute, GeoMesaStats.defaultBounds(ct.runtimeClass))
          }
        } else if (response == 3) {
          noBounds.foreach { attribute =>
            val ct = sft.getDescriptor(attribute).getType.getBinding
            var lower: Any = null
            var upper: Any = null
            while (lower == null) {
              lower = FastConverter.convert(Prompt.read(s"Enter initial lower bound for '$attribute': "), ct)
              if (lower == null) {
                Command.user.error(s"Couldn't convert input to appropriate type: ${ct.getSimpleName}")
              }
            }
            while (upper == null) {
              upper = FastConverter.convert(Prompt.read(s"Enter initial upper bound for '$attribute': "), ct)
              if (upper == null) {
                Command.user.error(s"Couldn't convert input to appropriate type: ${ct.getSimpleName}")
              }
            }
            if (lower == upper) {
              bounds.put(attribute, Histogram.buffer(lower))
            } else {
              bounds.put(attribute, (lower, upper))
            }
          }
        } else {
          Command.user.info("Operation cancelled.")
          return // cancel operation
        }
      }

      Command.user.info("Running stat query...")
      val length = bins.getOrElse(GeoMesaStats.DefaultHistogramSize)
      val queries = attributes.map { attribute =>
        val ct = ClassTag[Any](sft.getDescriptor(attribute).getType.getBinding)
        val (lower, upper) = bounds(attribute)
        Stat.Histogram[Any](attribute, length, lower, upper)(ct)
      }
      ds.stats.runStats[Histogram[Any]](sft, Stat.SeqStat(queries), filter)
    } else {
      if (filter != Filter.INCLUDE) {
        Command.user.warn("Ignoring CQL filter for non-exact stat query")
      }
      ds.stats.getStats[Histogram[Any]](sft, attributes).map {
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

    attributes.foreach { attribute =>
      histograms.find(_.property == attribute) match {
        case None => Command.user.info(s"No histogram available for attribute '$attribute'")
        case Some(hist) =>
          if (classOf[Geometry].isAssignableFrom(sft.getDescriptor(attribute).getType.getBinding)) {
            Command.output.info(StatsHistogramCommand.geomHistToString(attribute, hist.asInstanceOf[Histogram[Geometry]]))
          } else {
            StatsHistogramCommand.printHist(hist, sft, attribute)
          }
      }
    }

    ds.dispose()
  }
}

// @Parameters(commandDescription = "View or calculate counts of attribute in a GeoMesa feature type, grouped by sorted values")
trait StatsHistogramParams extends StatsParams with AttributeStatsParams {
  @Parameter(names = Array("--bins"), description = "How many bins the data will be divided into. " +
      "For example, if you are examining a week of data, you may want to divide the date into 7 bins, one per day.")
  var bins: Integer = _
}

object StatsHistogramCommand {

  /**
    * Creates a readable string for the histogram.
    */
  def printHist(stat: Histogram[Any], sft: SimpleFeatureType, attribute: String): Unit = {
    Command.output.info(s"Binned histogram for '$attribute':")
    if (stat.isEmpty) {
      Command.output.info("  No values")
    } else {
      val stringify = Stat.stringifier(sft.getDescriptor(attribute).getType.getBinding)
      (0 until stat.length).foreach { i =>
        val (min, max) = stat.bounds(i)
        Command.output.info(s"  [ ${stringify(min)} to ${stringify(max)} ] ${stat.count(i)}")
      }
    }
  }

  /**
    * Creates string containing an ASCII, color-coded map of densities.
    */
  def geomHistToString(attribute: String, stat: Histogram[Geometry]): String = {
    // grid of counts, corresponds to our world map dimensions
    val counts = Array.fill[Array[Long]](AsciiWorldMapHeight)(Array.fill[Long](AsciiWorldMapLength)(0))

    // translate histogram values into the grid and also calculate min/max for normalization
    def putCountsInGrid(): Unit = {
      var i = 0
      while (i < stat.length) {
        val count = stat.count(i)
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

    putCountsInGrid()

    // min/max to normalize our densities
    val min = counts.map(_.min).min
    val max = counts.map(_.max).max

    // normalize a count to 0-1 based on our min/max values
    def normalize(count: Long): Float = (count - min).toFloat / (max - min)
    // reverse a normalized percent
    def denormalize(percent: Float): Long = (percent * (max - min)).toLong + min

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
