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
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.tools.accumulo.commands.CommandWithCatalog
import org.locationtech.geomesa.utils.stats.{EnumerationStat, Stat, TopK}
import org.opengis.filter.Filter

import scala.math.Ordering

class StatsTopKCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  override val command = "stats-top-k"
  override val params = new StatsTopKParameters

  override def execute() = {
    val sft = ds.getSchema(params.featureName)
    val attributes = StatsCommand.getAttributes(sft, params)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val k = Option(params.k).map(_.intValue)

    val results = if (params.exact) {
      logger.info("Running stat query...")
      val query = Stat.SeqStat(attributes.map(Stat.TopK))
      ds.stats.runStats[TopK[Any]](sft, query, filter)
    } else {
      ds.stats.getStats[TopK[Any]](sft, attributes)
    }

    attributes.foreach { attribute =>
      println(s"Top values for '$attribute':")
      val stat = results.find(_.attribute == sft.indexOf(attribute))
      stat match {
        case None => println("  unavailable")
        case Some(s) =>
          val binding = sft.getDescriptor(attribute).getType.getBinding
          val ordering = StatsTopKCommand.ordering(binding)
          val stringify = Stat.stringifier(binding)
          s.topK(k.getOrElse(s.size)).sorted(ordering).foreach { case (value, count) =>
            println(s"  ${stringify(value)} ($count)")
          }
      }
    }
  }
}

@Parameters(commandDescription = "Enumerate the most frequent values in a GeoMesa feature type")
class StatsTopKParameters extends StatsParams with AttributeStatsParams {
  @Parameter(names = Array("-k"), description = "Number of top values to show")
  var k: Integer = null
}

object StatsTopKCommand {

  def ordering(binding: Class[_]): Ordering[Tuple2[Any, Long]] = {
    if (classOf[Comparable[Any]].isAssignableFrom(binding)) {
      new Ordering[Tuple2[Any, Long]] {
        override def compare(x: (Any, Long), y: (Any, Long)): Int = {
          // swap positions to get reverse sorting with large counts first
          val compareCount = y._2.compareTo(x._2)
          if (compareCount != 0) {
            compareCount
          } else {
            x._1.asInstanceOf[Comparable[Any]].compareTo(y._1)
          }
        }
      }
    } else {
      new Ordering[Tuple2[Any, Long]] {
        override def compare(x: (Any, Long), y: (Any, Long)): Int = {
          // swap positions to get reverse sorting with large counts first
          y._2.compareTo(x._2)
        }
      }
    }
  }

}