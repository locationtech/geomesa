/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import com.beust.jcommander.Parameter
import org.geotools.data.DataStore
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.locationtech.geomesa.utils.stats.{Stat, TopK}
import org.opengis.filter.Filter

import scala.math.Ordering

trait StatsTopKCommand[DS <: DataStore with HasGeoMesaStats] extends DataStoreCommand[DS] {

  override val name = "stats-top-k"
  override val params: StatsTopKParams

  override def execute(): Unit = withDataStore(topK)

  protected def topK(ds: DS): Unit = {
    val sft = ds.getSchema(params.featureName)
    val attributes = StatsCommand.getAttributesFromParams(sft, params)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val k = Option(params.k).map(_.intValue)

    val results = if (params.exact) {
      Command.user.info("Running stat query...")
      val query = Stat.SeqStat(attributes.map(Stat.TopK))
      ds.stats.runStats[TopK[Any]](sft, query, filter)
    } else {
      ds.stats.getStats[TopK[Any]](sft, attributes)
    }

    attributes.foreach { attribute =>
      Command.output.info(s"Top values for '$attribute':")
      val stat = results.find(_.attribute == sft.indexOf(attribute))
      stat match {
        case None => Command.output.info("  unavailable")
        case Some(s) =>
          val binding = sft.getDescriptor(attribute).getType.getBinding
          val ordering = StatsTopKCommand.ordering(binding)
          val stringify = Stat.stringifier(binding)
          s.topK(k.getOrElse(s.size)).sorted(ordering).foreach { case (value, count) =>
            Command.output.info(s"  ${stringify(value)} ($count)")
          }
      }
    }

    ds.dispose()
  }
}

// @Parameters(commandDescription = "Enumerate the most frequent values in a GeoMesa feature type")
trait StatsTopKParams extends StatsParams with AttributeStatsParams {
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
