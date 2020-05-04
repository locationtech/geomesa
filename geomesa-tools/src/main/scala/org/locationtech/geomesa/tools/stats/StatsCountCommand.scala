/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import com.beust.jcommander.ParameterException
import org.geotools.data.DataStore
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.tools.stats.StatsCountCommand.StatsCountParams
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.opengis.filter.Filter

trait StatsCountCommand[DS <: DataStore with HasGeoMesaStats] extends DataStoreCommand[DS] {

  override val name = "stats-count"
  override def params: StatsCountParams

  override def execute(): Unit = withDataStore(count)

  protected def count(ds: DS): Unit = {
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist")
    }

    val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)

    if (params.exact) {
      Command.user.info("Running stat query...")
      val count = ds.stats.getCount(sft, filter, params.exact).map(_.toString).getOrElse("Unknown")
      Command.output.info(s"Count: $count")
    } else {
      ds.stats.getCount(sft, filter, params.exact).map(_.toString) match {
        case None =>
          Command.output.info("Estimated count: Unknown")
          Command.output.info("Re-run with --no-cache to get an exact count")

        case Some(count) =>
          Command.output.info(s"Estimated count: $count")
      }
    }
  }
}

object StatsCountCommand {
  // @Parameters(commandDescription = "Estimate or calculate feature counts in a GeoMesa feature type")
  trait StatsCountParams extends StatsParams
}
