/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands.stats

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.tools.accumulo.commands.CommandWithCatalog
import org.locationtech.geomesa.utils.stats.{EnumerationStat, Stat}
import org.opengis.filter.Filter

class StatsEnumerateCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  override val command = "stats-enumerate"
  override val params = new StatsEnumerateParameters

  override def execute() = {
    val sft = ds.getSchema(params.featureName)
    val attributes = StatsCommand.getAttributes(sft, params)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)

    logger.info("Running stat query...")

    val query = Stat.SeqStat(attributes.map(Stat.Enumeration))
    val enumerations = ds.stats.runStats[EnumerationStat[Any]](sft, query, filter)

    attributes.foreach { attribute =>
      println(s"Values for '$attribute':")
      val enumeration = enumerations.find(_.attribute == sft.indexOf(attribute))
      enumeration match {
        case None => println("  unavailable")
        case Some(e) =>
          val stringify = Stat.stringifier(sft.getDescriptor(attribute).getType.getBinding)
          e.frequencies.toSeq.sortBy(_._2)(Ordering.Long.reverse).foreach { case (value, count) =>
            println(s"  ${stringify(value)} ($count)")
          }
      }
    }
  }
}

@Parameters(commandDescription = "Enumerate attribute values in a GeoMesa feature type")
class StatsEnumerateParameters extends StatsParams with AttributeStatsParams
