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
import org.opengis.filter.Filter

class StatsCountCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {

  override val command = "stats-count"
  override val params = new StatsCountParameters

  override def execute() = {
    val sft = ds.getSchema(params.featureName)
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)

    if (params.exact) {
      logger.info("Running stat query...")
    }

    val count = ds.stats.getCount(sft, filter, params.exact).map(_.toString).getOrElse("Unknown")

    val label = if (params.exact) "Count" else "Estimated count"
    println(s"$label: $count")
  }
}

@Parameters(commandDescription = "Estimate or calculate feature counts in a GeoMesa feature type")
class StatsCountParameters extends StatsParams with CachedStatsParams