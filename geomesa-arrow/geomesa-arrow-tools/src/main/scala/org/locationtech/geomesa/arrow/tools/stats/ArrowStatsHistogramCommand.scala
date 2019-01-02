/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.tools.stats

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.arrow.data.ArrowDataStore
import org.locationtech.geomesa.arrow.tools.ArrowDataStoreCommand
import org.locationtech.geomesa.arrow.tools.UrlParam
import org.locationtech.geomesa.tools.stats.{StatsHistogramCommand, StatsHistogramParams}

class ArrowStatsHistogramCommand extends StatsHistogramCommand[ArrowDataStore] with ArrowDataStoreCommand {

  override val params = new ArrowStatsHistogramParams

  override def execute(): Unit = {
    params.exact = true
    super.execute()
  }
}

@Parameters(commandDescription = "Calculate counts of attribute in a GeoMesa feature type, grouped by sorted values")
class ArrowStatsHistogramParams extends StatsHistogramParams with UrlParam
