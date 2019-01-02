/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.stats

import org.geotools.data.DataStore
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.tools.{Command, DataStoreCommand}
import org.opengis.filter.Filter

trait StatsCountCommand[DS <: DataStore with HasGeoMesaStats] extends DataStoreCommand[DS] {

  override val name = "stats-count"
  override def params: StatsCountParams

  override def execute(): Unit = withDataStore(count)

  protected def count(ds: DS): Unit = {
    val sft = ds.getSchema(params.featureName)
    val filter = Option(params.cqlFilter).getOrElse(Filter.INCLUDE)

    if (params.exact) {
      Command.user.info("Running stat query...")
    }

    val count = ds.stats.getCount(sft, filter, params.exact).map(_.toString).getOrElse("Unknown")

    val label = if (params.exact) "Count" else "Estimated count"
    Command.output.info(s"$label: $count")
  }
}

// @Parameters(commandDescription = "Estimate or calculate feature counts in a GeoMesa feature type")
trait StatsCountParams extends StatsParams
